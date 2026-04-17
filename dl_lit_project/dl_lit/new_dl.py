import argparse
import copy
import hashlib
import json
import os
import pprint
import re
import sqlite3
import subprocess
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from queue import Queue
from urllib.parse import quote_plus, urljoin, urlparse

import bibtexparser
import colorama
import requests
from bs4 import BeautifulSoup

from .db_manager import DatabaseManager
from .pdf_downloader import PDFDownloader
from .utils import ServiceRateLimiter, get_global_rate_limiter
try:
    # bibtexparser 1.x does not ship middlewares; keep optional so downloads can run without it.
    from bibtexparser.middlewares import SeparateCoAuthors, LatexEncodingMiddleware  # type: ignore
except Exception:  # pragma: no cover
    SeparateCoAuthors = None
    LatexEncodingMiddleware = None
colorama.init(autoreset=True) # Makes colors reset automatically
# BibtexParserError import removed for debugging, will be re-added once identified.
GREEN = colorama.Fore.GREEN
RED = colorama.Fore.RED
YELLOW = colorama.Fore.YELLOW
RESET = colorama.Style.RESET_ALL # Use colorama's reset
DOWNLOAD_DEBUG_ENABLED = os.getenv("RAG_FEEDER_DOWNLOAD_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}


def _download_log(message: str) -> None:
    print(message, flush=True)


def _download_debug(message: str) -> None:
    if DOWNLOAD_DEBUG_ENABLED:
        _download_log(f"[DL DEBUG] {message}")


def _download_debug_payload(label: str, payload) -> None:
    if DOWNLOAD_DEBUG_ENABLED:
        formatted = pprint.pformat(payload, compact=True, width=120, sort_dicts=False)
        _download_log(f"[DL DEBUG] {label}: {formatted}")

# Optional: only needed for Vertex AI flows. Keep import soft so the downloader
# can run in lightweight environments (like the web backend container).
try:
    import google.cloud.aiplatform as aiplatform  # type: ignore
except Exception:  # pragma: no cover
    aiplatform = None

from google import genai
# Removed incompatible google.genai.types imports


class BibliographyEnhancer:
    def __init__(self, db_manager: DatabaseManager, rate_limiter: ServiceRateLimiter, email: str = None, output_folder: str | Path = None, proxies: dict = None):
        self.email = email if email else 'spott@wzb.eu'
        self.unpaywall_headers = {'email': self.email}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Use injected dependencies for database and rate limiting
        self.db_manager = db_manager
        self.rate_limiter = rate_limiter
        
        # PROJECT_ID = "your-project-id"
        # vertexai.init()
        
        # Initialize credentials
        #credentials_path = "decision-theater-ffc892202832.json"
        #if not os.path.exists(credentials_path):
        #   raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
        #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                
        self.script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        
        # Initialize Vertex AI client
        #self.client = genai.Client(
        #    vertexai=True, project='decision-theater', location='europe-west2'
        #)

        # Set model ID
        self.MODEL_ID = "gemini-2.5-flash"

        api_key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
        self.client = genai.Client(api_key=api_key) if api_key else None
        # Google Search for grounding (disabled for compatibility)
        # self.google_search_tool = Tool(google_search=GoogleSearch())
        self.proxies = self._resolve_vpn_proxies(proxies)
        fallback_sources_raw = os.getenv("RAG_FEEDER_VPN_FALLBACK_SOURCES", "doi,publisher")
        self.vpn_fallback_sources = {
            token.strip().lower()
            for token in fallback_sources_raw.split(",")
            if token.strip()
        }
        self.vpn_mode = os.getenv("RAG_FEEDER_VPN_MODE", "fallback").strip().lower()
        self.vpn_enforce_eduvpn = os.getenv("RAG_FEEDER_VPN_ENFORCE_EDUVPN", "1").strip().lower() in {"1", "true", "yes", "on"}
        self.vpn_status_cmd = (os.getenv("RAG_FEEDER_VPN_STATUS_CMD") or "").strip()
        self.vpn_connect_cmd = (os.getenv("RAG_FEEDER_VPN_CONNECT_CMD") or "").strip()
        self.vpn_status_timeout_seconds = max(2, int(os.getenv("RAG_FEEDER_VPN_STATUS_TIMEOUT_SECONDS", "8") or "8"))
        self.vpn_connect_timeout_seconds = max(5, int(os.getenv("RAG_FEEDER_VPN_CONNECT_TIMEOUT_SECONDS", "45") or "45"))
        self.vpn_status_cache_ttl_seconds = max(1, int(os.getenv("RAG_FEEDER_VPN_STATUS_CACHE_SECONDS", "20") or "20"))
        self._vpn_status_cache_until = 0.0
        self._vpn_status_cache_value = False
        self._vpn_warned_missing_status_cmd = False
        self.enable_unpaywall = os.getenv("RAG_FEEDER_USE_UNPAYWALL", "1").strip().lower() in {"1", "true", "yes", "on"}
        self.download_timeout = max(3, int(os.getenv("RAG_FEEDER_DOWNLOAD_TIMEOUT_SECONDS", "12") or "12"))
        self.max_libgen_candidates = max(1, int(os.getenv("RAG_FEEDER_MAX_LIBGEN_CANDIDATES", "12") or "12"))
        self.output_folder = Path(output_folder) if output_folder else None
        self._pdf_downloader = None
        # Legacy helper methods in this module still expect both names.
        self.conn = getattr(self.db_manager, "conn", None)
        self.db_conn = self.conn
        
        # Initialize Sci-Hub mirror rotation
        self.current_mirror_index = 0
        if self.proxies:
            print(f"[DL] VPN proxy fallback enabled for sources: {sorted(self.vpn_fallback_sources)}")
            if self.vpn_enforce_eduvpn:
                if self.vpn_status_cmd:
                    print("[DL] eduVPN enforcement enabled: proxy use requires a connected status check.")
                else:
                    print("[DL] WARNING: RAG_FEEDER_VPN_ENFORCE_EDUVPN=1 but RAG_FEEDER_VPN_STATUS_CMD is not set; proxy route will be refused.")

    @staticmethod
    def _looks_like_bot_challenge(html: str) -> bool:
        text = (html or "").lower()
        markers = (
            "captcha",
            "security check",
            "attention required",
            "cloudflare",
            "verify you are human",
            "cf-challenge",
            "g-recaptcha",
            "hcaptcha",
            "/cdn-cgi/challenge-platform/",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def _classify_libgen_no_table_page(html: str) -> tuple[str, str]:
        text = html or ""
        lower = text.lower()
        if BibliographyEnhancer._looks_like_bot_challenge(text):
            return ("bot_detection", "challenge markers present")

        badge_counts = [int(match) for match in re.findall(r'badge\s+badge-primary">\s*(\d+)\s*<', text, re.I)]
        if badge_counts:
            if any(count > 0 for count in badge_counts):
                return ("non_file_results_present", f"no file table; badge counts={badge_counts}")
            return ("no_search_results", "all visible result badges are zero")

        if "no files found" in lower or "nothing found" in lower or "no results" in lower:
            return ("no_search_results", "page reports no results")

        return ("no_results_table", "no recognized results table or challenge markers")

    @staticmethod
    def _is_supported_libgen_result(url: str, source: str) -> bool:
        host = urlparse(url).netloc.lower()
        source_l = (source or "").strip().lower()
        blocked_hosts = (
            "randombook.org",
            "libgen.pw",
            "bookfi.net",
            "annas-archive",
        )
        if any(host_fragment in host for host_fragment in blocked_hosts):
            return False
        return True

    def _save_libgen_debug_response(
        self,
        *,
        base_url: str,
        context: str,
        query: str,
        response: requests.Response | None = None,
        error: str | None = None,
    ) -> None:
        debug_dir = Path("/home/jay/DT/logs/libgen_debug")
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        host = urlparse(base_url).netloc.replace(":", "_")
        digest = hashlib.sha1(f"{host}|{context}|{query}".encode("utf-8", "ignore")).hexdigest()[:10]
        path = debug_dir / f"{stamp}_{host}_{context}_{digest}.html"
        payload = []
        payload.append(f"<!-- base_url: {base_url} -->")
        payload.append(f"<!-- context: {context} -->")
        payload.append(f"<!-- query: {query} -->")
        if error:
            payload.append(f"<!-- error: {error} -->")
        if response is not None:
            payload.append(f"<!-- status: {response.status_code} -->")
            payload.append(f"<!-- final_url: {response.url} -->")
            payload.append(f"<!-- content_type: {response.headers.get('Content-Type', '')} -->")
            body = response.text
        else:
            body = ""
        payload.append(body)
        try:
            path.write_text("\n".join(payload), encoding="utf-8", errors="ignore")
            print(f"  Saved LibGen debug page: {path}")
        except Exception as exc:
            print(f"  Failed to save LibGen debug page: {exc}")

    @staticmethod
    def _response_looks_html(response: requests.Response) -> bool:
        content_type = (response.headers.get('Content-Type') or "").lower()
        if 'html' in content_type or content_type.startswith('text/'):
            return True
        sample = (response.content or b"")[:512].lstrip().lower()
        return sample.startswith(b'<!doctype html') or sample.startswith(b'<html') or b'<body' in sample

    def _extract_pdf_link_from_response(self, response: requests.Response):
        soup = BeautifulSoup(response.text, 'html.parser')

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if self.is_likely_pdf_url(href):
                print(f"  Found PDF link: {href}")
                found_link = href
                break
        else:
            found_link = None

        if not found_link:
            get_button = soup.find('a', href=True, string='GET')
            if get_button:
                found_link = get_button['href']
                print(f"  Found GET button link: {found_link}")

        if not found_link:
            embed = soup.find('embed', {'type': 'application/pdf'})
            if embed and embed.get('src'):
                found_link = embed['src']
                print(f"  Found embedded PDF link: {found_link}")

        if not found_link:
            iframe = soup.find('iframe', {'id': 'pdf'})
            if iframe and iframe.get('src'):
                found_link = iframe['src']
                print(f"  Found iframe PDF link: {found_link}")

        if not found_link:
            return None

        if not found_link.startswith(('http://', 'https://')):
            found_link = urljoin(response.url, found_link)
        return found_link

    @staticmethod
    def _extract_author_surname(author) -> str:
        if isinstance(author, dict):
            family = (author.get('family') or '').strip()
            if family:
                return family
            raw = author.get('raw')
            if raw is None:
                return ''
            author = raw

        text = str(author or '').strip()
        if not text:
            return ''

        # Common raw author format is "Lastname, A.".
        if ',' in text:
            left = text.split(',', 1)[0].strip()
            if left:
                return left

        parts = text.split()
        return parts[-1].strip(',.') if parts else ''

    @staticmethod
    def _normalize_libgen_query_text(title: str, surname: str = "") -> str:
        text = (title or "").strip()
        if ":" in text:
            text = text.split(":", 1)[0].strip()
        text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
        text = re.sub(r"\s+", " ", text).strip()
        if surname:
            text = f"{text} {surname}".strip()
        return text

    @staticmethod
    def _normalize_download_source_label(source: str | None, fallback: str = "Direct URL") -> str:
        text = str(source or "").strip()
        if not text or text.lower() in {"none", "null", "unknown"}:
            return fallback
        return text

    @staticmethod
    def _is_direct_download_candidate(url: str | None) -> bool:
        if not url:
            return False
        parsed = urlparse(str(url).strip())
        return parsed.scheme in {"http", "https"}

    def _resolve_vpn_proxies(self, proxies: dict | None) -> dict | None:
        if proxies:
            return proxies
        proxy_url = (os.getenv("RAG_FEEDER_VPN_PROXY_URL") or "").strip()
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}

    @staticmethod
    def _is_publisher_host(host: str) -> bool:
        if not host:
            return False
        host = host.lower()
        publisher_markers = (
            "springer",
            "sciencedirect.com",
            "elsevier.com",
            "wiley.com",
            "tandfonline.com",
            "jstor.org",
            "nature.com",
            "acm.org",
            "ieeexplore.ieee.org",
            "oxfordacademic.com",
            "cambridge.org",
            "sagepub.com",
            "degruyter.com",
            "proquest.com",
            "ebscohost.com",
        )
        return any(marker in host for marker in publisher_markers)

    def _should_use_vpn_fallback(self, source: str | None, url: str | None) -> bool:
        if not self.proxies:
            return False
        source_l = (source or "").strip().lower()
        host = urlparse(url).netloc.lower() if url else ""

        if "doi" in self.vpn_fallback_sources and (source_l == "doi" or "doi.org" in host):
            return True
        if "publisher" in self.vpn_fallback_sources and self._is_publisher_host(host):
            return True
        if "direct" in self.vpn_fallback_sources and source_l == "direct url":
            return True
        if "scihub" in self.vpn_fallback_sources and "sci-hub" in source_l:
            return True
        if "libgen" in self.vpn_fallback_sources and "libgen" in source_l:
            return True
        return False

    @staticmethod
    def _should_retry_with_vpn_status(status_code: int) -> bool:
        return status_code in {401, 402, 403, 407, 451, 500, 502, 503, 504}

    def _request_get_direct(self, url: str, **kwargs) -> requests.Response:
        with requests.Session() as session:
            session.trust_env = False
            return session.get(url, **kwargs)

    def _request_get_vpn(self, url: str, **kwargs) -> requests.Response:
        if not self.proxies:
            raise requests.exceptions.RequestException("VPN proxy not configured")
        if not self._is_eduvpn_connected():
            raise requests.exceptions.RequestException(
                "eduVPN not connected (or could not be verified); refusing desktop proxy route"
            )
        with requests.Session() as session:
            session.trust_env = False
            session.proxies.update(self.proxies)
            return session.get(url, **kwargs)

    @staticmethod
    def _run_shell_cmd(command: str, timeout_seconds: int) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            output = "\n".join(part for part in [proc.stdout.strip(), proc.stderr.strip()] if part)
            return proc.returncode == 0, output
        except Exception as exc:
            return False, str(exc)

    @staticmethod
    def _status_output_indicates_connected(output: str, command_ok: bool) -> bool:
        text = (output or "").strip().lower()
        if "not connected" in text or "disconnected" in text:
            return False
        if "currently connected" in text or "connected to" in text:
            return True
        # Generic status-check commands may intentionally produce no stdout and only signal via exit code.
        if not text:
            return bool(command_ok)
        # Fallback for custom outputs without canonical phrasing.
        return bool(command_ok and "connected" in text)

    def _is_eduvpn_connected(self, force_refresh: bool = False) -> bool:
        if not self.proxies:
            return False
        if not self.vpn_enforce_eduvpn:
            return True

        now = time.monotonic()
        if not force_refresh and now < self._vpn_status_cache_until:
            return self._vpn_status_cache_value

        if not self.vpn_status_cmd:
            if not self._vpn_warned_missing_status_cmd:
                print("[DL] WARNING: Cannot verify eduVPN status because RAG_FEEDER_VPN_STATUS_CMD is not set.")
                self._vpn_warned_missing_status_cmd = True
            connected = False
        else:
            ok, output = self._run_shell_cmd(self.vpn_status_cmd, self.vpn_status_timeout_seconds)
            connected = self._status_output_indicates_connected(output, ok)

        if not connected and self.vpn_connect_cmd:
            print("[DL] eduVPN not connected; running RAG_FEEDER_VPN_CONNECT_CMD before proxy request...")
            connect_ok, connect_output = self._run_shell_cmd(self.vpn_connect_cmd, self.vpn_connect_timeout_seconds)
            if not connect_ok:
                snippet = (connect_output or "").replace("\n", " ")[:220]
                print(f"[DL] eduVPN connect command failed: {snippet}")
            if self.vpn_status_cmd:
                ok, output = self._run_shell_cmd(self.vpn_status_cmd, self.vpn_status_timeout_seconds)
                connected = self._status_output_indicates_connected(output, ok)

        self._vpn_status_cache_value = connected
        self._vpn_status_cache_until = now + self.vpn_status_cache_ttl_seconds
        return connected

    def _request_get(self, url: str, *, source: str | None = None, allow_vpn_fallback: bool = False, **kwargs) -> tuple[requests.Response, str]:
        fallback_ok = allow_vpn_fallback and self._should_use_vpn_fallback(source, url)
        if fallback_ok and self.vpn_mode in {"prefer", "force"}:
            try:
                response = self._request_get_vpn(url, **kwargs)
                return response, "vpn"
            except requests.exceptions.RequestException as vpn_exc:
                if self.vpn_mode == "force":
                    raise
                print(f"  VPN preferred but failed ({vpn_exc}); falling back to direct...")
        try:
            response = self._request_get_direct(url, **kwargs)
            if fallback_ok and self._should_retry_with_vpn_status(response.status_code):
                print(f"  Direct request returned HTTP {response.status_code}; retrying via VPN proxy...")
                try:
                    return self._request_get_vpn(url, **kwargs), "vpn"
                except requests.exceptions.RequestException as vpn_exc:
                    print(f"  VPN fallback failed: {vpn_exc}")
            return response, "direct"
        except requests.exceptions.RequestException:
            if fallback_ok:
                print("  Direct request failed; retrying via VPN proxy...")
                return self._request_get_vpn(url, **kwargs), "vpn"
            raise

    def _setup_database(self) -> None:
        """Ensure schema exists before legacy table operations."""
        try:
            self.db_manager._create_schema()
        except Exception:
            # Some callers only need the current connection and can proceed.
            return

    def _norm_doi(self, doi):
        try:
            return self.db_manager._normalize_doi(doi)
        except Exception:
            return None

    def _norm_title(self, title):
        try:
            return self.db_manager._normalize_text(title)
        except Exception:
            return None

    def _norm_authors(self, authors):
        try:
            return self.db_manager._normalize_authors_value(authors)
        except Exception:
            return None

    def _get_pdf_downloader(self, downloads_dir: Path) -> PDFDownloader:
        """Lazily create a PDFDownloader for the current download directory."""
        if not isinstance(downloads_dir, Path):
            downloads_dir = Path(downloads_dir)
        if self._pdf_downloader is None or self._pdf_downloader.download_dir != downloads_dir:
            self._pdf_downloader = PDFDownloader(
                download_dir=downloads_dir,
                rate_limiter=self.rate_limiter,
                mailto=self.email,
            )
        return self._pdf_downloader

    @staticmethod
    def _classify_http_status(status_code: int | None) -> str:
        if status_code == 403:
            return "forbidden"
        if status_code == 404:
            return "not_found"
        if status_code == 429:
            return "rate_limited"
        if status_code and 500 <= int(status_code) <= 599:
            return "server_error"
        return "http_error"

    @staticmethod
    def _classify_exception_message(message: str | None) -> str:
        text = (message or "").lower()
        if "timed out" in text or "timeout" in text:
            return "timeout"
        if "too many redirects" in text:
            return "too_many_redirects"
        if "forbidden" in text or "403" in text:
            return "forbidden"
        if "404" in text or "not found" in text:
            return "not_found"
        if "429" in text or "rate limit" in text:
            return "rate_limited"
        if "connection" in text:
            return "connection_error"
        if "invalid complete pdf" in text or "failed pdf validation" in text:
            return "invalid_pdf"
        if "no pdf link" in text:
            return "no_pdf_link"
        if "no doi" in text:
            return "no_doi"
        return "request_error"

    def _ensure_download_trace(self, ref: dict) -> list[dict]:
        if not isinstance(ref, dict):
            return []
        trace = ref.get("download_trace")
        if not isinstance(trace, list):
            trace = []
            ref["download_trace"] = trace
        return trace

    def _record_download_event(
        self,
        ref: dict,
        *,
        source: str,
        outcome: str,
        category: str | None = None,
        detail: str | None = None,
        url: str | None = None,
        route: str | None = None,
        duration_ms: int | None = None,
        phase: str = "download",
    ) -> None:
        trace = self._ensure_download_trace(ref)
        event = {
            "phase": phase,
            "source": source,
            "outcome": outcome,
        }
        if category:
            event["category"] = category
        if detail:
            event["detail"] = detail[:400]
        if url:
            event["url"] = url[:400]
        if route:
            event["route"] = route
        if duration_ms is not None:
            event["duration_ms"] = int(duration_ms)
        trace.append(event)
        if outcome != "success" and category:
            ref["download_failure_category"] = category
            if detail:
                ref["download_failure_detail"] = detail[:400]

    def escape_bibtex(self, text: str | None) -> str:
        """Escapes characters with special meaning in BibTeX/LaTeX."""
        if text is None:
            return ""
        # Simple replacements - may need refinement based on actual BibTeX processor
        replacements = {
            '\\' : r'\textbackslash{}', # Must be first
            '{'  : r'\{', 
            '}'  : r'\}',
            '$'  : r'\$',
            '&'  : r'\&',
            '%'  : r'\%',
            '#'  : r'\#',
            '_'  : r'\_',
            '^'  : r'\^{}', # Escaping caret often needs braces
            '~'  : r'\~{}', # Escaping tilde often needs braces
            '"' : r'{''}',  # Double quotes can cause issues
            # Add others if needed, e.g., accented characters if not using UTF-8 encoding properly
        }
        escaped_text = str(text) # Ensure it's a string
        for char, escaped_char in replacements.items():
            escaped_text = escaped_text.replace(char, escaped_char)
        return escaped_text



    def check_unpaywall(self, doi):
        if not doi:
            return None
        
        # Clean the DOI if it starts with doi: prefix
        if doi.lower().startswith('doi:'):
            doi = doi[4:].strip()
        
        # Extract the DOI part if it's a URL
        if doi.startswith(('http://', 'https://')):
            parsed = urlparse(doi)
            if 'doi.org' in parsed.netloc and parsed.path:
                doi = parsed.path[1:] if parsed.path.startswith('/') else parsed.path
            elif parsed.path.startswith('/10.'):
                doi = parsed.path[1:]
        
        # Check if the DOI starts with 10.
        if not doi.startswith('10.'):
            print(f"Invalid DOI format for Unpaywall: {doi} - DOI must start with '10.'")
            return None
        
        url = f'https://api.unpaywall.org/v2/{doi}?email={self.email}'
        
        try:
            # Check for rate limits including daily limit
            if not self.rate_limiter.wait_if_needed('unpaywall'):
                print(f"Unpaywall daily request limit exceeded. Skipping Unpaywall check for DOI {doi}.")
                return None
                
            response = self._request_get_direct(url, headers=self.unpaywall_headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for {doi}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {doi}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {doi}: {e}")
            return None

    def try_download(self, url_info, ref, downloads_dir):
        url = url_info['url']
        print(f"Trying {url_info['source']}: {url}")
        try:
            response, route_used = self._request_get(
                url,
                source=url_info.get('source'),
                allow_vpn_fallback=True,
                headers={'User-Agent': 'Mozilla/5.0'},
                timeout=self.download_timeout,
                allow_redirects=True,
            )
            if route_used == "vpn":
                print(f"  Using VPN route for {url_info.get('source', 'URL')}")
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'application/pdf' in content_type:
                    filename = self.save_pdf(response.content, ref, downloads_dir, url_info['source'])
                    if filename:
                        return {
                            'success': True,
                            'file': str(filename),
                            'source': url_info['source'],
                            'reason': url_info['reason']
                        }
                elif 'json' in content_type:
                    try:
                        data = response.json()
                        # Handle JSON response if it contains PDF links or other useful information
                        if 'pdf_url' in data:
                            url_info['url'] = data['pdf_url']
                            url_info['reason'] = f"{url_info['reason']} (via JSON response)"
                            return self.try_download(url_info, ref, downloads_dir)
                        return {'success': False, 'reason': 'JSON response did not contain PDF link'}
                    except json.JSONDecodeError:
                        print(f"JSON parse error for {url}: Response was not valid JSON")
                        return {'success': False, 'reason': 'JSON parse error'}
                else:
                    # Check if the response is HTML and look for PDF links
                    if 'html' in content_type or 'text' in content_type:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        pdf_link = None
                        
                        # Check embed tag first
                        embed = soup.find('embed', {'type': 'application/pdf'})
                        if embed and embed.get('src'):
                            pdf_link = embed['src']
                        
                        # If no embed, check iframe
                        if not pdf_link:
                            iframe = soup.find('iframe', {'id': 'pdf'})
                            if iframe and iframe.get('src'):
                                pdf_link = iframe['src']
                        
                        # If still no link, check for download button
                        if not pdf_link:
                            save_button = soup.find('button', string='↓ save')
                            if save_button:
                                onclick = save_button.get('onclick', '')
                                if "location.href='" in onclick:
                                    pdf_link = onclick.split("location.href='")[1].split("'")[0]
                        
                        # If we found any kind of link, process it
                        if pdf_link:
                            # Handle relative URLs
                            if not pdf_link.startswith(('http://', 'https://')):
                                if pdf_link.startswith('//'):
                                    pdf_link = f"https:{pdf_link}"
                                else:
                                    pdf_link = urljoin(url, pdf_link)
                            else:
                                pdf_link = urljoin(url, pdf_link)
                                    
                            print(f"Found PDF link via {url_info['source']}: {pdf_link}")
                            # Return dictionary when we find a working mirror
                            return {
                                'url': pdf_link,
                                'source': 'Sci-Hub',
                                'reason': f"DOI resolved to PDF via {url_info['source']}"
                            }
                        
                        print(f"No PDF link found via {url_info['source']}.")
                        return None
            else:
                return {'success': False, 'reason': f'HTTP {response.status_code}'}
        except requests.exceptions.RequestException as e:
            return {'success': False, 'reason': f'Request error: {str(e)}'}
        except Exception as e:
            return {'success': False, 'reason': f'Unexpected error: {str(e)}'}

    def search_with_scihub(self, doi):
        """
        Search for a paper on Sci-Hub using its DOI and retrieve the download link.
        Returns immediately after finding a working mirror.
        """
        if not doi:
            print("Sci-Hub: No DOI provided")
            return None
        
        # Clean and normalize the DOI first
        clean_doi = doi
        if clean_doi.startswith("doi:"):
            clean_doi = clean_doi[4:]  # Remove 'doi:' prefix
        if clean_doi.startswith("https://doi.org/") or clean_doi.startswith("http://doi.org/"):
            # Extract just the DOI part for Sci-Hub
            clean_doi = clean_doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
            
        print(f"Sci-Hub: Searching for DOI {clean_doi}")

        # Try multiple Sci-Hub mirrors
        scihub_mirrors = [
            "https://sci-hub.al",
            "https://www.tesble.com",
            "https://sci-hub.shop",
            "https://sci-hub.vg",
            "https://sci-hub.ren",
            "https://sci-hub.wf",
            "https://sci-hub.ee",
            "https://sci-hub.mksa.top"
        ]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
        }

        # Apply rate limiting before starting the loop
        if not self.rate_limiter.wait_if_needed('scihub'):
            print("Sci-Hub rate limit exceeded. Skipping...")
            return None

        # Implement mirror rotation: start with next mirror in rotation
        start_index = self.current_mirror_index
        self.current_mirror_index = (self.current_mirror_index + 1) % len(scihub_mirrors)
        
        # Try mirrors starting from current index
        for i in range(len(scihub_mirrors)):
            mirror_index = (start_index + i) % len(scihub_mirrors)
            mirror = scihub_mirrors[mirror_index]
            scihub_url = f"{mirror}/{clean_doi}"
            print(f"  Trying Sci-Hub mirror: {mirror}")
            
            start_req = time.monotonic()
            try:
                response = self._request_get_direct(scihub_url, headers=headers, timeout=self.download_timeout)
                req_duration = time.monotonic() - start_req
                _download_debug(f"Sci-Hub request to {mirror} took {req_duration:.2f}s")
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                # Parse the HTML response to find the PDF link
                pdf_link = None # Initialize pdf_link for this mirror attempt
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check embed tag first
                embed = soup.find('embed', {'type': 'application/pdf'})
                if embed and embed.get('src'):
                    pdf_link = embed['src']
                
                # If no embed, check iframe
                if not pdf_link:
                    iframe = soup.find('iframe', {'id': 'pdf'})
                    if iframe and iframe.get('src'):
                        pdf_link = iframe['src']
                
                # If still no link, check for download button
                if not pdf_link:
                    save_button = soup.find('button', string='↓ save')
                    if save_button:
                        onclick = save_button.get('onclick', '')
                        if "location.href='" in onclick:
                            pdf_link = onclick.split("location.href='")[1].split("'")[0]
                
                # If we found any kind of link, process it
                if pdf_link:
                    # Handle relative URLs
                    if not pdf_link.startswith(('http://', 'https://')):
                        if pdf_link.startswith('//'):
                            pdf_link = f"https:{pdf_link}"
                        elif pdf_link.startswith('/'):
                            pdf_link = f"{mirror}{pdf_link}"
                        else:
                            pdf_link = f"{mirror}/{pdf_link}"
                            
                    print(f"Found PDF link via {mirror}: {pdf_link}")
                    # Return dictionary when we find a working mirror
                    return {
                        'url': pdf_link,
                        'source': 'Sci-Hub',
                        'reason': f'DOI resolved to PDF via {mirror}'
                    }
                
                print(f"No PDF link found on {mirror}. Stopping search as content is likely the same across mirrors.")
                return None
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                print(f"Connection issue with {mirror}: {e}. Trying next mirror...")
            except requests.exceptions.RequestException as e:
                print(f"Error accessing {mirror}: {e}. Trying next mirror...")
        return None

    def search_libgen(self, title: str, author_surnames: list[str], ref_type: str = ''):
        """Search LibGen mirrors and return likely download links, prioritizing PDFs."""
        search_author = author_surnames[0] if author_surnames else ''
        is_book = (ref_type or '').lower() in ['book', 'monograph']

        query_text = self._normalize_libgen_query_text(title, search_author)
        query = quote_plus(query_text)
        libgen_mirrors = [
            "https://libgen.li",
            "https://libgen.vg",
        ]

        print(f"Searching LibGen for: {title}")
        print(f"  Book type detection: is_book={is_book}, ref_type='{ref_type}'")
        print(f"  Using author surname: '{search_author}'")
        print(f"  Normalized LibGen query: '{query_text}'")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
        }

        pdf_matches = []
        other_matches = []
        seen_urls = set()

        for base_url in libgen_mirrors:
            libgen_url = f"{base_url}/index.php?req={query}&lg_topic=libgen&open=0&view=simple&res=25&phrase=1&column=def"
            print(f"  LibGen URL: {libgen_url}")
            try:
                self.rate_limiter.wait_if_needed('libgen')
                response = self._request_get_direct(libgen_url, headers=headers, timeout=self.download_timeout)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # Prefer canonical file table, fallback to older variant if present.
                table = soup.find('table', {'id': 'tablelibgen'}) or soup.find('table', {'id': 'tablelibgen1'})
                if not table:
                    context, detail = self._classify_libgen_no_table_page(response.text)
                    print(f"  No LibGen results table found on {base_url} ({detail})")
                    if context == "bot_detection":
                        print(f"  WARNING: Bot/challenge page detected on {base_url}")
                    self._save_libgen_debug_response(
                        base_url=base_url,
                        context=context,
                        query=query,
                        response=response,
                    )
                    continue

                rows = table.find_all('tr')[1:]
                print(f"  {base_url}: found {len(rows)} candidate rows")

                for idx, row in enumerate(rows, start=1):
                    cells = row.find_all('td')
                    if len(cells) < 9:
                        print(f"  Row {idx}: skipped - only {len(cells)} cells")
                        continue

                    result_title = cells[0].get_text(strip=True)
                    result_author = cells[1].get_text(strip=True)
                    file_ext = cells[7].get_text(strip=True).lower()

                    if "Review by:" in result_author:
                        continue

                    if is_book:
                        review_markers = [
                            "vol.", "iss.", "pp.", "pages",
                            "Review of", "Book Review",
                            ") pp.", ") p.",
                        ]
                        if any(marker in result_title for marker in review_markers):
                            continue

                    mirrors_cell = cells[-1]
                    mirror_links = mirrors_cell.find_all('a')
                    for link in mirror_links:
                        href = link.get('href', '')
                        source = link.get('data-original-title', link.get_text(strip=True))
                        if not href:
                            continue

                        resolved_href = urljoin(base_url + '/', href)
                        if not self._is_supported_libgen_result(resolved_href, source):
                            continue
                        if resolved_href in seen_urls:
                            continue
                        seen_urls.add(resolved_href)

                        match_entry = {
                            'url': resolved_href,
                            'title': result_title,
                            'authors': result_author,
                            'source': f'LibGen ({urlparse(base_url).netloc}: {source})',
                            'reason': f'{file_ext.upper()} download',
                        }

                        if file_ext == 'pdf':
                            pdf_matches.append(match_entry)
                        else:
                            other_matches.append(match_entry)
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response and e.response.status_code == 429:
                    print(f"  LibGen rate limit exceeded on {base_url}, trying next mirror")
                    self._save_libgen_debug_response(
                        base_url=base_url,
                        context="http_429",
                        query=query,
                        response=e.response,
                        error=str(e),
                    )
                    continue
                if hasattr(e, 'response') and e.response and e.response.status_code in {500, 502, 503, 504}:
                    self._save_libgen_debug_response(
                        base_url=base_url,
                        context=f"http_{e.response.status_code}",
                        query=query,
                        response=e.response,
                        error=str(e),
                    )
                elif isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                    self._save_libgen_debug_response(
                        base_url=base_url,
                        context="request_error",
                        query=query,
                        error=str(e),
                    )
                print(f"  Error searching {base_url}: {str(e)}")
                continue

        matches = pdf_matches + other_matches
        print(f"Found {len(matches)} non-review LibGen links ({len(pdf_matches)} PDFs)")
        return matches


    def is_likely_pdf_url(self, url):
        """Checks if a URL is likely to point directly to a PDF based on patterns."""
        if not isinstance(url, str) or not url:
            return False
        url_lower = url.lower()
        
        # General PDF checks
        if any([
            url_lower.endswith('.pdf'),
            'pdf' in url_lower and '/download' in url_lower,
            'pdf' in url_lower and '/view' in url_lower,
            '/pdf/' in url_lower,
            'pdf' in url_lower and 'download' in url_lower, # Added check
        ]):
            return True
        
        return False
        
    def try_extract_pdf_link(self, html_url, source: str | None = None, allow_vpn_fallback: bool = False, force_vpn: bool = False):
        try:
            print(f"  Checking HTML page for PDF link: {html_url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15'
            }
            if force_vpn:
                response = self._request_get_vpn(html_url, headers=headers, timeout=self.download_timeout)
            else:
                response, route_used = self._request_get(
                    html_url,
                    source=source or "html",
                    allow_vpn_fallback=allow_vpn_fallback,
                    headers=headers,
                    timeout=self.download_timeout,
                )
                if route_used == "vpn":
                    print("  HTML page fetched via VPN route")
            response.raise_for_status()
            return self._extract_pdf_link_from_response(response)

        except requests.exceptions.RequestException as e:
            print(f"  Error checking HTML page: {e}")
            return None
        except Exception as e:
            print(f"  Unexpected error checking HTML page: {e}")
            return None
            
    def download_paper(self, url, filename, ref_type):
        """Enhanced paper downloader with better error handling"""
        if not url:
            return False
                        
        print(f"  Loading: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1',
                'Accept': 'text/html,application/pdf,application/x-pdf,*/*'
            }
            
            source_label = "DOI" if "doi.org" in urlparse(url).netloc.lower() else "Direct URL"
            response, route_used = self._request_get(
                url,
                source=source_label,
                allow_vpn_fallback=True,
                allow_redirects=True,
                headers=headers,
                timeout=self.download_timeout,
            )
            if route_used == "vpn":
                print("  Retried via VPN route")
            
            # Check for specific HTTP error codes
            if response.status_code == 404:
                print("  PDF not found (404)")
                return False
            elif response.status_code == 403:
                print("  Access forbidden (403)")
                return False
            elif response.status_code == 429:
                print("  Too many requests (429) - Rate limited")
                return False
            elif response.status_code != 200:
                print(f"  HTTP error {response.status_code}")
                return False
                    
            content = response.content
            
            # Enhanced PDF validation (shared)
            downloader = self._get_pdf_downloader(Path(filename).parent)
            if downloader.validate_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print("  Direct download successful - Valid complete PDF")
                return True
            else:
                print("  Downloaded file is not a valid complete PDF")
                
                # If it seems to be HTML, try extracting PDF link
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    pdf_url = self.try_extract_pdf_link(
                        url,
                        source=source_label,
                        allow_vpn_fallback=(route_used == "direct"),
                        force_vpn=(route_used == "vpn"),
                    )
                    if pdf_url and pdf_url != url:
                        return self.download_paper(pdf_url, filename, ref_type)
            
            return False
                
        except requests.exceptions.ConnectionError:
            print("  Connection error - Could not reach server")
            return False
        except requests.exceptions.Timeout:
            print("  Request timed out")
            return False
        except requests.exceptions.TooManyRedirects:
            print("  Too many redirects")
            return False
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {str(e)}")
            return False
        except Exception as e:
            print(f"  Unexpected error: {str(e)}")
            if filename.exists():
                filename.unlink()
            return False


    def clean_doi_url(self, doi):
        """Clean and format DOI URL correctly"""
        # Remove any duplicate doi.org prefixes
        doi = doi.replace('https://doi.org/https://doi.org/', 'https://doi.org/')
        doi = doi.replace('http://doi.org/http://doi.org/', 'https://doi.org/')
        
        # If it's just the DOI number, add the prefix
        if not doi.startswith('http'):
            doi = f"https://doi.org/{doi}"
            
        return doi


    def enhance_bibliography(self, ref, downloads_dir, force_download=False):
        """Enhances a single reference dictionary, attempts download, and returns the modified ref or None."""
        _download_debug_payload("ref received", ref)
        # Ensure downloads_dir is a Path object (it should be, but double-check)
        if not isinstance(downloads_dir, Path):
            _download_log(f"[DL ERROR] downloads_dir passed to enhance_bibliography is not a Path object: {type(downloads_dir)}")
            try:
                downloads_dir = Path(downloads_dir)
            except TypeError:
                _download_log("[DL ERROR] Cannot convert downloads_dir to Path. Aborting enhance_bibliography for this ref.")
                return None # Indicate failure


        # The CLI now handles the user-facing print, so it's removed from here.

        # Use ref directly since it already contains the data we need
        bib_entry = ref
        _download_debug_payload("bib_entry extracted", bib_entry)

        # Handle both original and enriched data structures
        if 'enriched_data' in bib_entry and bib_entry['enriched_data']:
            # Enriched structure from OpenAlex/Crossref
            enriched = bib_entry['enriched_data']
            title = enriched.get('title') or enriched.get('display_name', '')
            doi = enriched.get('doi')
            authorships = enriched.get('authorships', [])
            authors = [item.get('author', {}).get('display_name') for item in authorships if item.get('author', {}).get('display_name')] if authorships else []
            # Get open access URL from enriched data
            open_access_url = enriched.get('open_access', {}).get('oa_url') if enriched.get('open_access') else None

        elif 'original_reference' in bib_entry and bib_entry['original_reference']:
             # Fallback to original reference data
            original_ref = bib_entry['original_reference']
            title = original_ref.get('title', '')
            authors = original_ref.get('authors', [])
            doi = original_ref.get('doi', None)
            open_access_url = original_ref.get('open_access_url')
        else:
            # Extract directly from ref structure (this is the case we're in)
            title = bib_entry.get('title', '')
            authors = bib_entry.get('authors', [])
            doi = bib_entry.get('doi', None)
            open_access_url = bib_entry.get('open_access_url')

        # Ensure title and authors are not None
        title = title or ''
        authors = authors or []
        _download_debug(f"authors extracted: {authors}")
        
        ref_type = ref.get('type', '')
        urls_to_try = []
        download_success = False

        # Try consolidated downloader first (OpenAlex OA / primary_location / unpaywall if present)
        openalex_payload = bib_entry.get('openalex_json')
        if openalex_payload and isinstance(openalex_payload, str):
            try:
                openalex_payload = json.loads(openalex_payload)
            except json.JSONDecodeError:
                openalex_payload = None

        if not openalex_payload and open_access_url:
            openalex_payload = {"open_access": {"oa_url": open_access_url}}

        if openalex_payload:
            downloader = self._get_pdf_downloader(downloads_dir)
            path, checksum, source, attempts = downloader.attempt_download(openalex_payload, ref_type=ref_type)
            for attempt in attempts:
                self._record_download_event(
                    ref,
                    source=attempt.get("source") or "PDFDownloader",
                    outcome="success" if attempt.get("outcome") == "success" else "failure",
                    category=attempt.get("failure_category"),
                    detail=attempt.get("detail"),
                    url=attempt.get("url"),
                    phase="download",
                )
            if path:
                ref['downloaded_file'] = str(path)
                ref['download_source'] = self._normalize_download_source_label(source, fallback="OpenAlex OA URL")
                ref['download_reason'] = 'Downloaded via PDFDownloader'
                ref['checksum'] = checksum
                download_success = True
                if not force_download:
                    return ref

        # 1. Try direct URL first if it exists
        direct_url = bib_entry.get('url')
        if self._is_direct_download_candidate(direct_url):
            print("Found direct URL in reference")
            urls_to_try.append({
                'url': direct_url,
                'title': title,
                'snippet': '',
                'source': 'Direct URL',
                'reason': 'Direct URL from reference'
            })
        elif direct_url:
            _download_debug(f"Skipping non-HTTP direct URL candidate: {direct_url}")
        
        # 1. Try DOI resolution
        if doi:
            doi_url = self.clean_doi_url(doi)
            urls_to_try.append({
                'url': doi_url,
                'title': title,
                'snippet': '',
                'source': 'DOI',
                'reason': 'DOI resolution'
            })
            
            # 1. Check Unpaywall for open access versions (optional; disabled by default for speed)
            if self.enable_unpaywall:
                unpaywall_result = self.check_unpaywall(doi)
                if unpaywall_result and unpaywall_result.get('best_oa_location'):
                    oa_location = unpaywall_result['best_oa_location']
                    if oa_location.get('url_for_pdf'):
                        print(f"  Unpaywall found OA PDF URL: {oa_location['url_for_pdf']}")
                        urls_to_try.append({
                            'url': oa_location['url_for_pdf'],
                            'title': title,
                            'snippet': '',
                            'source': oa_location.get('host_type') or 'Unpaywall OA',
                            'reason': 'Found via Unpaywall DOI lookup'
                        })
                    else:
                        print(f"  Unpaywall found OA location but no direct PDF URL for DOI {doi}.")
                        self._record_download_event(ref, source="Unpaywall", outcome="failure", category="no_pdf_url", detail=f"No direct PDF URL for DOI {doi}", phase="lookup")
                else:
                     print(f"  Unpaywall found no OA location for DOI {doi}.")
                     self._record_download_event(ref, source="Unpaywall", outcome="failure", category="no_oa_location", detail=f"No OA location for DOI {doi}", phase="lookup")
            else:
                print("  Unpaywall lookup disabled (RAG_FEEDER_USE_UNPAYWALL=0)")

        if open_access_url:
            urls_to_try.append({
                'url': open_access_url,
                'title': title,
                'snippet': '',
                'source': 'OpenAlex',
                'reason': 'Open access URL from OpenAlex'
            })
        
        # Try downloading from direct sources first
        for url_info in urls_to_try:
            if self.try_download_url(url_info['url'], url_info['source'], ref, downloads_dir):
                ref['download_source'] = self._normalize_download_source_label(url_info['source'], fallback='Direct URL')
                ref['download_reason'] = url_info['reason']
                download_success = True
                file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                print(f"{GREEN}✓ Downloaded from: {ref['download_source']} as {Path(file_path).name}{RESET}")

        # 2. If direct methods fail, try Sci-Hub (using DOI)
        if not download_success and ref.get('doi'):
            print("Direct methods failed. Trying Sci-Hub...")
            scihub_result = self.search_with_scihub(ref['doi'])
            
            if scihub_result:
                print(f"\nFound PDF link on Sci-Hub: {scihub_result['url']}")
                if self.try_download_url(scihub_result['url'], scihub_result['source'], ref, downloads_dir):
                    ref['download_source'] = self._normalize_download_source_label(scihub_result['source'], fallback='Sci-Hub')
                    ref['download_reason'] = scihub_result['reason'] # Keep Sci-Hub reason
                    download_success = True
                    file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                    print(f"{GREEN}✓ Downloaded from: {ref['download_source']} as {Path(file_path).name}{RESET}")
            else:
                self._record_download_event(ref, source="Sci-Hub", outcome="failure", category="no_match", detail=f"No PDF link found for DOI {ref['doi']}", phase="lookup")

        # 3. If Sci-Hub fails (or no DOI), try LibGen
        if not download_success:
            # Extract surnames for LibGen search
            author_surnames = []
            if authors:
                for author in authors:
                    surname = self._extract_author_surname(author)
                    if surname:
                        author_surnames.append(surname)

            print("Sci-Hub failed (or no DOI). Trying LibGen...")
            libgen_results = self.search_libgen(title, author_surnames, ref.get('type', ''))
            if libgen_results:
                print(f"Found {len(libgen_results)} results on LibGen")
                for idx, url_info in enumerate(libgen_results):
                    if self.max_libgen_candidates and idx >= self.max_libgen_candidates:
                        print(f"Reached LibGen candidate limit ({self.max_libgen_candidates}); stopping LibGen attempts for this entry.")
                        break
                    libgen_source = url_info.get('source', "LibGen")
                    if self.try_download_url(url_info['url'], libgen_source, ref, downloads_dir):
                        ref['download_source'] = self._normalize_download_source_label(libgen_source, fallback='LibGen')
                        ref['download_reason'] = 'Successfully downloaded via LibGen'
                        download_success = True
                        file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                        print(f"{GREEN}✓ Downloaded from: {ref['download_source']} as {Path(file_path).name}{RESET}")
                        break
            else:
                print("No LibGen matches found.")
                self._record_download_event(ref, source="LibGen", outcome="failure", category="no_match", detail=f"No LibGen matches for {title[:160]}", phase="lookup")


        # If all methods fail, mark as not found
        if not download_success:
            print(f"  {RED}✗ No valid matches found through any method{RESET}")
            ref['download_status'] = 'not_found'
            ref['download_reason'] = 'No valid matches found through DOI, Unpaywall, Sci-Hub, or LibGen'
            ref.setdefault('download_failure_category', 'source_exhausted')
            ref.setdefault('download_failure_detail', ref['download_reason'])

        # Return the modified ref dictionary if download was successful, otherwise None
        if 'downloaded_file' in ref and ref['downloaded_file']:
             # Optionally, update DB status here if enhance_bibliography is solely responsible
             # update_download_status(ref, self.db_conn, downloaded=True)
             return ref # Return the modified ref dict on success
        else:
             # Optionally, update DB status here if enhance_bibliography is solely responsible
             # update_download_status(ref, self.db_conn, downloaded=False)
             return None # Return None on failure or skip


    def try_download_url(self, url, source, ref, downloads_dir, _force_vpn=False, _vpn_retry_used=False):
        """Attempts to download a PDF from a given URL.
        Includes basic validation and HTML page handling.
        Returns True if successful, False otherwise. Sets ref['downloaded_file'] on success.
        """
        # Safely get title, provide fallback, ensure string
        title_val = (ref or {}).get('title', '') or 'Untitled'
        if not isinstance(title_val, str):
            print(f"{RED}ERROR try_download_url: Title is not a string for ref ID {ref.get('id', 'N/A')}! Using fallback.{RESET}")
            title_val = 'Untitled_check_source_data'

        _download_debug(f"try_download_url source={source} url={url}")

        # Create safe filename from title
        safe_title = "".join(c for c in (title_val[:50]) if c.isalnum() or c in ' -_').strip()
        filename = downloads_dir / f"{ref.get('year', 'unknown')}_{safe_title}.pdf"
        ref_type = ref.get('type', '')
        
        print(f"\nTrying {source}: {url}")
        started_at = time.monotonic()
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1',
                'Accept': 'text/html,application/pdf,application/x-pdf,*/*'
            }
            
            # Add timeout to request
            if _force_vpn:
                response = self._request_get_vpn(url, allow_redirects=True, headers=headers, timeout=self.download_timeout)
                route_used = "vpn"
            else:
                response, route_used = self._request_get(
                    url,
                    source=source,
                    allow_vpn_fallback=True,
                    allow_redirects=True,
                    headers=headers,
                    timeout=self.download_timeout,
                )
            if route_used == "vpn":
                print("  Request route: VPN")
            
            # Check for specific HTTP error codes
            if response.status_code == 404:
                print("  PDF not found (404)")
                self._record_download_event(ref, source=source, outcome="failure", category="not_found", detail="HTTP 404", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                return False
            elif response.status_code == 403:
                print("  Access forbidden (403)")
                self._record_download_event(ref, source=source, outcome="failure", category="forbidden", detail="HTTP 403", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                return False
            elif response.status_code == 429:
                print("  Too many requests (429) - Rate limited")
                self._record_download_event(ref, source=source, outcome="failure", category="rate_limited", detail="HTTP 429", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                return False
            elif response.status_code != 200:
                print(f"  HTTP error {response.status_code}")
                self._record_download_event(ref, source=source, outcome="failure", category=self._classify_http_status(response.status_code), detail=f"HTTP {response.status_code}", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                return False
                    
            content = response.content
            content_type = (response.headers.get('Content-Type') or "").lower()
            
            # Enhanced PDF validation (shared)
            downloader = self._get_pdf_downloader(downloads_dir)
            if downloader.validate_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print("  Direct download successful - Valid complete PDF")
                ref['downloaded_file'] = str(filename)
                normalized_source = self._normalize_download_source_label(source, fallback="Direct URL")
                effective_source = normalized_source if route_used == "direct" else f"{normalized_source} (via VPN)"
                ref['download_source'] = effective_source
                ref['download_route'] = route_used
                self._record_download_event(ref, source=effective_source, outcome="success", detail="direct_download", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                return True
            else:
                print(f"  {YELLOW}Downloaded file is not a valid complete PDF{RESET}")
                self._record_download_event(ref, source=source, outcome="failure", category="invalid_pdf", detail="Downloaded file failed PDF validation", url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)

                if (
                    route_used == "direct"
                    and not _vpn_retry_used
                    and self._should_use_vpn_fallback(source, url)
                ):
                    print("  Direct route returned non-PDF content; retrying same URL via VPN...")
                    return self.try_download_url(
                        url,
                        source,
                        ref,
                        downloads_dir,
                        _force_vpn=True,
                        _vpn_retry_used=True,
                    )

                # If it seems to be HTML, try extracting PDF link
                # --- MODIFICATION HERE ---
                pdf_url_from_html = None
                if self._response_looks_html(response):
                    pdf_url_from_html = self._extract_pdf_link_from_response(response)
                elif any(token in url.lower() for token in ('ads.php', '/book/', 'sci-hub.', 'doi.org')):
                    pdf_url_from_html = self.try_extract_pdf_link(
                        url,
                        source=source,
                        allow_vpn_fallback=(route_used == "direct"),
                        force_vpn=(route_used == "vpn"),
                    )
                if pdf_url_from_html and pdf_url_from_html != url: # Avoid infinite loops
                    print(f"  Found potential PDF link in HTML, retrying download for: {pdf_url_from_html}")
                    # Recursively call try_download_url with the new link, potentially updating source
                    return self.try_download_url(
                        pdf_url_from_html,
                        source + " (via HTML)",
                        ref,
                        downloads_dir,
                        _force_vpn=(route_used == "vpn"),
                        _vpn_retry_used=_vpn_retry_used,
                    )
                else:
                     print(f"  {YELLOW}Could not extract a PDF link from the HTML page.{RESET}")
                     if "libgen" in (source or "").lower():
                         self._save_libgen_debug_response(
                             base_url=url,
                             context="no_pdf_link",
                             query=title_val[:120],
                             response=response if self._response_looks_html(response) else None,
                             error=f"source={source}",
                         )
                     detail = f"No PDF link extracted from response (content-type={content_type or 'unknown'})"
                     self._record_download_event(ref, source=f"{source} HTML", outcome="failure", category="no_pdf_link", detail=detail, url=url, route=route_used, duration_ms=(time.monotonic() - started_at) * 1000)
                # --- END MODIFICATION ---

            return False # Return False if download wasn't valid PDF and no link extracted/retried

        except requests.exceptions.ConnectionError:
            print("  Connection error - Could not reach server")
            self._record_download_event(ref, source=source, outcome="failure", category="connection_error", detail="Connection error", url=url, duration_ms=(time.monotonic() - started_at) * 1000)
            return False
        except requests.exceptions.Timeout:
            print("  Request timed out")
            self._record_download_event(ref, source=source, outcome="failure", category="timeout", detail="Request timed out", url=url, duration_ms=(time.monotonic() - started_at) * 1000)
            return False
        except requests.exceptions.TooManyRedirects:
            print("  Too many redirects")
            self._record_download_event(ref, source=source, outcome="failure", category="too_many_redirects", detail="Too many redirects", url=url, duration_ms=(time.monotonic() - started_at) * 1000)
            return False
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {str(e)}")
            self._record_download_event(ref, source=source, outcome="failure", category=self._classify_exception_message(str(e)), detail=str(e), url=url, duration_ms=(time.monotonic() - started_at) * 1000)
            return False
        except Exception as e:
            print(f"  Unexpected error: {str(e)}")
            if filename.exists():
                try:
                    filename.unlink()
                    print(f"  Removed potentially incomplete file: {filename.name}")
                except Exception as unlink_e:
                     print(f"  Error removing file during exception handling: {unlink_e}")
            self._record_download_event(ref, source=source, outcome="failure", category="unexpected_error", detail=str(e), url=url, duration_ms=(time.monotonic() - started_at) * 1000)
            return False

    # --- BibTeX Export --- <<< NEW SECTION

    def _format_bibtex_entry(self, entry_id, metadata_json):
        """Formats a dictionary representing a DB row into a BibTeX entry string,
           using the raw metadata JSON.
        """
        try:
            # Parse the metadata JSON
            try:
                entry_data = json.loads(metadata_json)
                if not isinstance(entry_data, dict):
                    raise ValueError("Metadata is not a JSON object")
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                 print(f"[BIBTEX FORMAT WARN] Error parsing metadata JSON for entry ID {entry_id}: {e}")
                 return None # Cannot format without valid metadata

            # --- Filter: Only include if downloaded_file exists ---
            file_path = entry_data.get('downloaded_file')
            if not file_path or not str(file_path).strip(): # Check if None, empty string, or just whitespace
                 # print(f"[BIBTEX SKIP] Skipping entry ID {entry_id} (title: {entry_data.get('title', 'N/A')[:30]}...) - No download path.") # Optional debug
                 return None

            # --- BibTeX Key Generation ---
            # Start with DOI if available
            bib_key = (entry_data.get('doi') or '').strip()
            if bib_key:
                # Clean DOI for use as key (remove prefix, replace separators)
                bib_key = re.sub(r'^https?://(dx\\.)?doi\\.org/', '', bib_key)
                bib_key = bib_key.replace('/', '_').replace('.', '-')
            else:
                # Fallback: Use hash of title+first_author+year
                title_str = entry_data.get('title', '')
                authors_val = entry_data.get('authors', [])
                first_author = ''
                authors_list_for_key = [] # Use a different variable name
                # Directly check if it's a list, no json.loads needed here
                if isinstance(authors_val, list):
                    authors_list_for_key = authors_val

                # Process the list if it's not empty
                if authors_list_for_key:
                    # Handle if author item is dict (OpenAlex format) or simple string
                    author_item = authors_list_for_key[0]
                    if isinstance(author_item, dict):
                         first_author = author_item.get('display_name', author_item.get('name', ''))
                    elif isinstance(author_item, str):
                         first_author = author_item
                # else: first_author remains ''

                year_str = str(entry_data.get('year', entry_data.get('publication_year', ''))).strip()

                if title_str or first_author or year_str:
                    hash_input = f"{title_str}|{first_author}|{year_str}".encode('utf-8')
                    hash_object = hashlib.sha1(hash_input)
                    bib_key = f"ref_{hash_object.hexdigest()[:10]}" # Use 'ref_' prefix + 10 hex chars
                else:
                     # Absolute fallback: Use database ID
                     bib_key = f"entry{entry_id}"

            # Final sanitization (remove non-alphanumeric, underscores, hyphens allowed)
            bib_key = re.sub(r'[^a-zA-Z0-9_-]+', '', bib_key)
            # Ensure key is not empty after sanitization
            if not bib_key:
                 bib_key = f"entry{entry_id}_Sanitized"

            # --- Entry Type ---
            # Get the entry type directly from metadata without mapping
            bibtex_type = (entry_data.get('type') or 'misc').lower().strip()
            # Sanitize type for BibTeX (remove any unsafe characters)
            bibtex_type = re.sub(r'[^a-z0-9]', '', bibtex_type)
            # Ensure we have a valid type
            if not bibtex_type:
                bibtex_type = 'misc'

            # --- Fields ---
            fields = []

            # Title (required for most types)
            title = entry_data.get('title')
            if title:
                fields.append(f"  title = {{{{{self.escape_bibtex(title)}}}}}")
            # else: Allow entries without titles for now

            # Authors
            authors_val = entry_data.get('authors', [])
            authors_list_processed = []
            # Directly check if it's a list
            if isinstance(authors_val, list):
                 authors_list_processed = authors_val
            # Optional: Add handling here if authors might be a string needing parsing
            # elif isinstance(authors_val, str): ...

            author_names = []
            if isinstance(authors_list_processed, list): # Check it's a list before iterating
                for author_item in authors_list_processed:
                     name = None
                     if isinstance(author_item, dict):
                         # Prioritize display_name, fallback to name
                         name = author_item.get('display_name', author_item.get('name'))
                     elif isinstance(author_item, str):
                         name = author_item
                     if name:
                          author_names.append(self.escape_bibtex(name))

            if author_names:
                 authors_field_str = " and ".join(author_names)
                 fields.append(f"  author = {{{{{authors_field_str}}}}}") # Corrected closing braces

            # Year / Publication Year
            year = entry_data.get('year', entry_data.get('publication_year'))
            if year:
                try:
                    # Ensure year is integer-like before adding
                    year_str = str(int(year)) # Attempt conversion to int first
                    fields.append(f"  year = {{{{{year_str}}}}}") # Corrected closing braces
                except (ValueError, TypeError):
                    print(f"[BIBTEX WARN] Entry ID {entry_id}: Non-integer year '{year}' found, skipping year field.")

            # DOI
            doi = entry_data.get('doi')
            if doi:
                 cleaned_doi = str(doi).strip()
                 # Ensure it starts with http or add prefix
                 if cleaned_doi.startswith('http'):
                      pass # Keep URL as is
                 elif cleaned_doi: # If not empty and not starting with http
                      cleaned_doi = f"https://doi.org/{cleaned_doi}"

                 if cleaned_doi: # Add if not empty after processing
                      fields.append(f"  doi = {{{{{self.escape_bibtex(cleaned_doi)}}}}}") # Corrected closing braces

            # Journal / Booktitle / Series / Publisher based on type
            container = entry_data.get('container_title', entry_data.get('source')) # Use 'source' as fallback
            publisher = entry_data.get('publisher')

            if container:
                container_escaped = self.escape_bibtex(container)
                if bibtex_type == 'article':
                    fields.append(f"  journal = {{{{{container_escaped}}}}}") # Corrected closing braces
                elif bibtex_type in ['incollection', 'inproceedings', 'inbook']:
                    fields.append(f"  booktitle = {{{{{container_escaped}}}}}") # Corrected closing braces
                # Add other mappings if needed (e.g., series for techreport)
                # elif bibtex_type == 'techreport' and not publisher:
                #    fields.append(f"  series = {{{{{container_escaped}}}}}")

            if publisher:
                 # Add publisher for relevant types
                 if bibtex_type in ['book', 'inbook', 'incollection', 'proceedings', 'techreport']:
                     fields.append(f"  publisher = {{{{{self.escape_bibtex(publisher)}}}}}") # Corrected closing braces

            # Volume
            volume = entry_data.get('volume')
            if volume and bibtex_type in ['article', 'incollection', 'inproceedings', 'book', 'inbook']:
                 # Check if volume is not just whitespace
                 volume_str = str(volume).strip()
                 if volume_str:
                     fields.append(f"  volume = {{{{{self.escape_bibtex(volume_str)}}}}}") # Corrected closing braces

            # Issue/Number
            issue = entry_data.get('issue')
            if issue and bibtex_type in ['article']:
                 # Check if issue is not just whitespace
                 issue_str = str(issue).strip()
                 if issue_str:
                     fields.append(f"  number = {{{{{self.escape_bibtex(issue_str)}}}}}") # 'number' is often used for issue, Corrected closing braces

            # Pages
            pages = entry_data.get('pages')
            if pages and bibtex_type in ['article', 'incollection', 'inproceedings', 'inbook']:
                 # Standardize page range separator to double hyphen and check if not empty
                 pages_str = str(pages).replace('-', '--').strip()
                 if pages_str:
                     fields.append(f"  pages = {{{{{self.escape_bibtex(pages_str)}}}}}") # Corrected closing braces

            # Abstract (optional - can make BibTeX file large)
            abstract = entry_data.get('abstract')
            if abstract:
               fields.append(f"  abstract = {{{{{self.escape_bibtex(abstract)}}}}}") # Corrected closing braces

            # File link (using the path checked earlier)
            # file_path was retrieved at the start for filtering
            if file_path:
                 try:
                     # Ensure self.output_folder is a Path object and exists
                     if not hasattr(self, 'output_folder') or not isinstance(self.output_folder, Path):
                          # This should be set in export_table_to_bibtex
                          raise AttributeError("output_folder attribute not set or not a Path object in _format_bibtex_entry")

                     file_abs_path = Path(file_path)
                     # Check if file actually exists before trying to make relative
                     if file_abs_path.exists():
                         relative_path = file_abs_path.relative_to(self.output_folder)
                         fields.append(f"  file = {{{{{str(relative_path)}}}:PDF}}") # Common BibDesk/JabRef format, Corrected closing braces
                     else:
                         print(f"[BIBTEX WARN] Entry ID {entry_id}: File path '{file_path}' in metadata does not exist. Skipping file field.")

                 except (ValueError, AttributeError, TypeError) as e:
                      # Use absolute path if relative fails or path isn't valid
                      print(f"[BIBTEX WARN] Entry ID {entry_id}: Failed to create relative file path ({e}). Using absolute path.")
                      try:
                          abs_path_str = str(Path(file_path).resolve())
                          if Path(abs_path_str).exists():
                              fields.append(f"  file = {{{{{abs_path_str}}}:PDF}}") # Corrected closing braces
                          else:
                              print(f"[BIBTEX WARN] Entry ID {entry_id}: Absolute file path '{abs_path_str}' does not exist. Skipping file field.")
                      except Exception as path_e:
                           print(f"[BIBTEX ERROR] Entry ID {entry_id}: Could not resolve absolute file path '{file_path}': {path_e}")

            # --- Assemble Entry ---
            # Stricter check: Skip if no title AND no authors
            if not title and not author_names:
                 print(f"[BIBTEX SKIP] Entry ID {entry_id}: Skipping due to missing both title and authors.")
                 return None

            bibtex_entry = f"@{bibtex_type}{{{bib_key},\n" # Use real newlines
            bibtex_entry += ",\n".join(fields)
            bibtex_entry += "\n}" # Use real newline

            return bibtex_entry

        except Exception as e:
            print(f"[BIBTEX FORMAT ERROR] Error formatting BibTeX for entry ID {entry_id}: {e}")
            import traceback
            traceback.print_exc() # Print full traceback for debugging
            return None # Return None on error
            
    def export_table_to_bibtex(self, table_name, output_folder):
        """Exports all entries from a specified table to a BibTeX file."""
        output_dir = Path(output_folder)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Always use 'metadata.bib' in the downloads subfolder
        downloads_dir = output_dir / "downloads"
        downloads_dir.mkdir(exist_ok=True)
        bib_file = downloads_dir / "metadata.bib"
        count = 0
        failed_count = 0
        cursor = self.db_conn.cursor()
        
        # Set the output_folder attribute for use in _format_bibtex_entry
        # This is crucial for creating relative file paths
        self.output_folder = output_dir
        
        try:
            # Fetch metadata JSON and DB type for correct mapping
            cursor.execute(f"SELECT id, metadata, type FROM {table_name}")
            bib_file.unlink(missing_ok=True)
            for entry_id, metadata_json, db_entry_type in cursor.fetchall():
                # Merge DB type into metadata JSON
                try:
                    entry_data = json.loads(metadata_json) if metadata_json else {}
                except Exception:
                    entry_data = {}
                if db_entry_type:
                    entry_data['type'] = db_entry_type
                merged_json = json.dumps(entry_data)
                entry = self._format_bibtex_entry(entry_id, merged_json)
                if entry:
                    count += 1
                    with open(bib_file, 'a', encoding='utf-8') as f:
                        f.write(entry + "\n\n")
                else:
                    failed_count += 1
        except sqlite3.Error as e:
            print(f"[BIBTEX EXPORT ERROR] Database error reading from {table_name}: {e}")
        print(f"Exported {count} BibTeX entries from {table_name} to {bib_file}.")
        if failed_count > 0:
            print(f"Failed to format or write {failed_count} entries.")

    # ----------------------------------------------------------------------
    # Download stage: process refs and move from input to current
    # ----------------------------------------------------------------------
    def process_references_from_database(self, output_dir: str | Path, max_concurrent: int = 10,
                                         force_download: bool = False,
                                         skip_referenced_works: bool = False) -> int:
        """Download all refs in input_references, move to current_references immediately."""
        # Ensure DB tables are present
        self._setup_database()
        self.output_folder = Path(output_dir) # Store output folder path
        self.output_folder.mkdir(parents=True, exist_ok=True)
        downloads_dir = self.output_folder / 'downloads'
        downloads_dir.mkdir(parents=True, exist_ok=True)

        cursor = self.db_conn.cursor()
        print(f"[PROCESS] Fetching references from input_references table...")
        # Fetch rowid along with metadata to uniquely identify rows for deletion later
        cursor.execute("SELECT rowid, metadata FROM input_references")
        rows = cursor.fetchall()
        print(f"[PROCESS] Found {len(rows)} references to process.")

        work_q = Queue()
        for rowid, metadata_json in rows:
            try:
                ref_data = json.loads(metadata_json)
                # Add the original rowid to the data structure being passed around
                ref_data['_original_rowid'] = rowid
                work_q.put(ref_data)
            except json.JSONDecodeError:
                print(f"[WARN] Invalid JSON metadata in input_references for rowid {rowid}: {metadata_json[:100]}...")

        if work_q.empty():
            print("[PROCESS] No references in the input queue to process.")
            return 0

        processed = 0
        active = {}
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # start initial batch
            for _ in range(min(max_concurrent, work_q.qsize())):
                if not work_q.empty():
                    r = work_q.get()
                    future = executor.submit(self.enhance_bibliography, copy.deepcopy(r), downloads_dir, force_download)
                    active[future] = r

            # process queue
            while active:
                done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)
                for fut in done:
                    ref = active.pop(fut)
                    try:
                        result = fut.result()
                        ok = result is not None
                        entry = result or ref
                    except Exception as e:
                        print(f"{RED}!!! Error processing {ref.get('title','Untitled')}: {e}{RESET}")
                        ok = False
                        entry = ref

                    processed += 1
                    if processed % 5 == 0 or processed == work_q.qsize():
                        print(f"Processed {processed}/{work_q.qsize()} references...")

                    # prepare normalized keys
                    doi = (entry.get('doi','') or '').strip().lower()
                    norm_title = self._norm_title(entry.get('title', ''))
                    norm_auth = self._norm_authors(entry.get('authors', []))
                    meta = json.dumps(entry, ensure_ascii=False)

                    # Get the database path for the dedicated connection
                    db_path = self.db_conn.execute("PRAGMA database_list;").fetchone()[2]

                    with sqlite3.connect(db_path, timeout=10) as dedicated_conn:
                        cur2 = dedicated_conn.cursor()
                        if ok:
                            cur2.execute(
                                """
                                INSERT OR IGNORE INTO current_references
                                  (doi,title,year,authors,metadata,normalized_doi,normalized_title,normalized_authors)
                                VALUES (?,?,?,?,?,?,?,?)
                                """,
                                (
                                    entry.get('doi'), entry.get('title'), str(entry.get('year','')),
                                    json.dumps(entry.get('authors',[]), ensure_ascii=False), meta,
                                    doi, norm_title, norm_auth
                                )
                            )
                        # Always remove the original row from input_references using its unique rowid
                        if '_original_rowid' in entry:
                            cur2.execute("DELETE FROM input_references WHERE rowid = ?", (entry['_original_rowid'],))
                        else:
                            # Fallback logic (should ideally not be needed if rowid is always passed)
                            print(f"[WARN] Missing '_original_rowid' for entry: {entry.get('title')}. Attempting delete by normalized keys.")
                            if doi:
                                cur2.execute("DELETE FROM input_references WHERE normalized_doi = ?", (doi,))
                            elif norm_title and norm_auth:
                                cur2.execute(
                                    "DELETE FROM input_references WHERE normalized_title = ? AND normalized_authors = ?",
                                    (norm_title, norm_auth)
                                )
                        dedicated_conn.commit()

                    # enqueue next
                    if not work_q.empty():
                        nr = work_q.get()
                        nf = executor.submit(self.enhance_bibliography, copy.deepcopy(nr), downloads_dir, force_download)
                        active[nf] = nr

        print("\nFinished processing all references.")
        self.export_table_to_bibtex('current_references', self.output_folder)

        return processed
    def import_legacy_metadata(self, metadata_dir: str | Path):
        """Import references from older JSON files into the 'previous_references' table."""
        metadata_path = Path(metadata_dir)
        if not metadata_path.is_dir():
            print(f"[ERROR] Legacy metadata directory not found: {metadata_dir}")
            return

        print(f"[DB] Importing legacy metadata from: {metadata_path}")
        json_files = list(metadata_path.rglob('*.json'))
        total_files = len(json_files)
        imported_count = 0
        skipped_count = 0
        error_count = 0
        commit_batch_size = 1000 # Commit every 1000 inserts
        
        cursor = self.conn.cursor()

        for i, file_path in enumerate(json_files):
            if (i + 1) % 100 == 0:
                 print(f"  [Legacy Import] Processing file {i+1}/{total_files}: {file_path.name}...")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Handle both list and dictionary structures
                reference_entries = []
                if isinstance(data, list):
                    # List structure: each item is a reference, use list index as position
                    reference_entries = [(None, ref, idx) for idx, ref in enumerate(data)]
                elif isinstance(data, dict):
                    # Dictionary structure: keys are filenames/identifiers, values are references
                    reference_entries = [(key, ref, 0) for key, ref in data.items()]
                else:
                    print(f"  [WARN] Skipping {file_path.name}: Expected top-level list or dict, got {type(data)}")
                    error_count += 1 # Count file-level errors
                    continue

                # Iterate through the prepared entries (filename_or_none, ref, default_position)
                for entry_filename, ref, default_pos in reference_entries:
                    # Ensure the value is a dictionary (the reference object)
                    if not isinstance(ref, dict):
                        identifier = entry_filename if entry_filename else f"entry #{default_pos}"
                        print(f"  [WARN] Skipping {identifier} in {file_path.name}: Expected dict value, got {type(ref)}")
                        error_count += 1 # Count reference-level errors
                        continue

                    # --- Start of processing for a single reference 'ref' --- 
                    # Normalize fields for checking and insertion
                    norm_doi = self._norm_doi(ref.get('doi'))
                    norm_title = self._norm_title(ref.get('title'))
                    norm_authors = self._norm_authors(ref.get('authors'))
                    year = ref.get('year') # Keep original 'year' if present
                    metadata_json = json.dumps(ref) # Store the original JSON for this specific reference
                    # Use the dict key as filename if available, otherwise use JSON filename
                    if entry_filename:
                        filename = entry_filename
                    else:
                        filename = str(file_path.relative_to(metadata_path))
                    # Use position from ref if available, otherwise use default_pos
                    position = default_pos
                    if 'position' in ref:
                       try:
                           position = int(ref['position'])
                       except (ValueError, TypeError):
                           position = default_pos # Default if conversion fails
                    
                    # Extract new fields (handle potential missing keys) for this ref
                    abstract = ref.get('abstract')
                    pub_year = ref.get('publication_year') # Use 'publication_year' field if available
                    ref_type = ref.get('type')
                    container = ref.get('container_title')

                    # Check if already exists based on normalized fields in previous_references
                    if self._row_exists('previous_references', norm_doi, norm_title, norm_authors):
                        skipped_count += 1
                        continue

                    # Insert into previous_references for this ref
                    initial_changes = self.conn.total_changes
                    try:
                         # Corrected INSERT statement with 14 columns and 14 placeholders
                         cursor.execute(""" 
                            INSERT OR IGNORE INTO previous_references 
                            (doi, title, year, authors, abstract, publication_year, type, container_title, metadata, filename, position, normalized_doi, normalized_title, normalized_authors)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                        """, (ref.get('doi'), ref.get('title'), year, json.dumps(ref.get('authors')), 
                              abstract, pub_year, ref_type, container, # New values
                              metadata_json, filename, position, norm_doi, norm_title, norm_authors))
                         
                         if self.conn.total_changes > initial_changes:
                              imported_count += 1
                              # Add OpenAlex ID to set if imported successfully and present
                              work_id_url = ref.get("id")
                              if isinstance(work_id_url, str) and work_id_url.startswith('https://openalex.org/W'):
                                  self.existing_work_ids.add(work_id_url)
                         else:
                              skipped_count += 1
                    except sqlite3.Error as insert_e:
                        identifier = entry_filename if entry_filename else f"entry #{default_pos}"
                        print(f"  [ERROR] Failed to insert record for {identifier} from {file_path.name}: {insert_e}")
                        error_count += 1
                    # --- End of processing for a single reference 'ref' ---

                    # Commit in batches based on imported_count (references, not files)
                    if imported_count > 0 and imported_count % commit_batch_size == 0:
                        try:
                            self.conn.commit()
                            print(f"  [Legacy Import] Committed batch of {commit_batch_size} imports.")
                        except sqlite3.Error as commit_e:
                            print(f"  [ERROR] Failed to commit batch: {commit_e}")

            except json.JSONDecodeError:
                print(f"  [ERROR] Failed to decode JSON from {file_path.name}")
                error_count += 1 # Count file-level errors
            except Exception as e:
                print(f"  [ERROR] Unexpected error processing {file_path.name}: {e}")
                error_count += 1 # Count file-level errors
        # Final commit for any remaining records
        try:
            self.conn.commit()
            print(f"[DB] Finished legacy import. Imported={imported_count}, Skipped={skipped_count}, Errors={error_count}")
        except sqlite3.Error as commit_e:
             print(f"  [ERROR] Failed to commit final batch: {commit_e}")

    def add_reference(self, ref: dict, table: str = "input_references") -> bool:
        """Adds a reference dictionary to the specified table, checking for duplicates.
        Returns True if the reference was added, False if skipped (duplicate).
        Handles basic normalization and updates the in-memory work ID set.
        Commits should happen outside this method, after batches.
        """
        if table not in ["input_references", "previous_references", "current_references"]:
            print(f"[DB ERROR] Invalid table name: {table}")
            return False

        # Handle both original and enriched data structures
        if 'enriched_data' in ref and ref['enriched_data']:
            # Enriched structure from OpenAlex/Crossref
            enriched = ref['enriched_data']
            title = enriched.get('title') or enriched.get('display_name', '')
            doi = enriched.get('doi')
            year = enriched.get('publication_year')
            authorships = enriched.get('authorships', [])
            authors = [item.get('author', {}).get('display_name') for item in authorships if item.get('author', {}).get('display_name')] if authorships else []

        elif 'original_reference' in ref and ref['original_reference']:
             # Fallback to original reference data
            original_ref = ref['original_reference']
            title = original_ref.get('title', '')
            authors = original_ref.get('authors', [])
            year = original_ref.get('year', None)
            doi = original_ref.get('doi', None)
        else:
            # Fallback for entries that were not enriched (e.g. from bibtex import)
            title = ref.get('title', '')
            authors = ref.get('authors', [])
            year = ref.get('year', None)
            doi = ref.get('doi', None)

        # Ensure title and authors are not None
        title = title or ''
        authors = authors or []
        meta = json.dumps(ref) # Store original metadata
        norm_doi = self._norm_doi(doi)
        norm_title = self._norm_title(title)
        norm_authors = self._norm_authors(authors)
        # Get filename/position if available in ref (e.g., from primary JSON ingestion)
        filename = ref.get('filename') 
        position = ref.get('position') 
        
        # Extract new fields
        abstract = ref.get('abstract')
        pub_year = ref.get('publication_year')
        ref_type = ref.get('type')
        container = ref.get('container_title')

        # --- Duplicate Check Logic --- 
        # Primary check: Does this OpenAlex ID already exist anywhere in memory?
        work_id_url = ref.get("id")
        if isinstance(work_id_url, str) and work_id_url.startswith('https://openalex.org/W'):
            if work_id_url in self.existing_work_ids:
                return False # Skip if OpenAlex ID exists in memory

        # Secondary check: Use _row_exists? Let's rely on INSERT OR IGNORE for now.

        cursor = self.conn.cursor()
        initial_changes = self.conn.total_changes

        try:
            # Insert, relying on INSERT OR IGNORE and potential UNIQUE constraints
            # Updated INSERT with 14 columns
            cursor.execute(f"""INSERT OR IGNORE INTO {table}
                           (doi, title, year, authors, abstract, publication_year, type, container_title, metadata, filename, position, normalized_doi, normalized_title, normalized_authors)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """,
                           (doi, title, year, json.dumps(authors), 
                            abstract, pub_year, ref_type, container, # New values
                            meta, filename, position, norm_doi, norm_title, norm_authors))
            
            # Check if a row was actually inserted
            if self.conn.total_changes > initial_changes:
                 # If inserted and it has an OpenAlex ID, add it to our memory set
                 if isinstance(work_id_url, str) and work_id_url.startswith('https://openalex.org/W'):
                     self.existing_work_ids.add(work_id_url)
                 return True # Row was inserted
            else:
                 return False # Row was ignored (duplicate based on DB constraints)

        except sqlite3.Error as e:
            print(f"[DB ERROR] Failed to insert into {table}: {e}")
            print(f"  Reference data: DOI='{norm_doi}', Title='{norm_title[:50]}...' ")
            return False
        # NO COMMIT HERE

    def close(self):
        """Closes the database connection."""
        try:
            if self.db_manager:
                self.db_manager.close_connection()
                print("[DB] Connection closed.")
        except Exception:
            return

    # ------------------------------------------------------------------
    # DB schema adjustments – add normalised columns if missing (DEPRECATED) 
    # Now handled in __init__ by creating tables with all columns.
    # ------------------------------------------------------------------
    # def _ensure_columns(self):
    # ... (keeping the old method commented out for reference if needed) ...

# ----------------------------------------------------------------------
# OpenAlex batch fetch helper – keeps rate-limiters & batch logic together
# ----------------------------------------------------------------------
def fetch_openalex_batch(work_ids: list[str], fields: str = "id,doi,display_name,authorships,publication_year,title,referenced_works,abstract_inverted_index", email: str = "default@example.com", limiter: ServiceRateLimiter | None = None) -> list[dict]:
    """Fetch metadata for a list of OpenAlex IDs using the batch filter API, ≤100 IDs per call."""
    if not work_ids:
        return []
    if limiter is None:
        # Use the global rate limiter shared across all components
        limiter = get_global_rate_limiter()

    all_results: list[dict] = []
    batch_size = 50 # OpenAlex recommended max batch size for filter
    total_batches = (len(work_ids) + batch_size - 1) // batch_size
    
    print(f"[OpenAlex] Preparing to fetch {len(work_ids)} IDs in {total_batches} batches.")

    for idx in range(0, len(work_ids), batch_size):
        batch_ids = work_ids[idx : idx + batch_size]
        current_batch_num = (idx // batch_size) + 1
        
        if not limiter.wait_if_needed():
            print("[OpenAlex ERROR] Daily quota exceeded – stopping fetch early.")
            break
            
        # Construct filter string like 'W123|W456'
        filter_param = "|".join(batch_ids)
        url = (
            f"https://api.openalex.org/works?filter=openalex_id:{filter_param}&select={fields}&per-page={len(batch_ids)}&mailto={email}"
        )

        try:
            print(f"  [OpenAlex] Fetching Batch {current_batch_num}/{total_batches} ({len(batch_ids)} IDs)...")
            resp = requests.get(url, headers={"User-Agent": f"ReferenceIngestor/1.0 ({email})"}, timeout=60)
            resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = resp.json()
            batch_res = data.get("results", [])
            print(
                f"    [OpenAlex] Batch {current_batch_num}/{total_batches}: Got {len(batch_res)} results."
            )
            all_results.extend(batch_res)
        except requests.exceptions.Timeout:
            print(f"    [OpenAlex WARN] Timeout occurred for Batch {current_batch_num} - skipping.")
        except requests.exceptions.HTTPError as e:
             print(f"    [OpenAlex WARN] HTTP Error for Batch {current_batch_num}: {e} - skipping.")
             # Maybe add specific handling for 429 Too Many Requests if needed
        except requests.exceptions.RequestException as e:
            print(f"    [OpenAlex WARN] Request error for Batch {current_batch_num}: {e} - skipping.")
        except json.JSONDecodeError:
            print(f"    [OpenAlex WARN] Invalid JSON response for Batch {current_batch_num} – skipping.")
            
    print(f"[OpenAlex] Finished fetching. Total results received: {len(all_results)}")
    return all_results

# ----------------------------------------------------------------------
# Helper to convert OpenAlex abstract format
# ----------------------------------------------------------------------
def convert_inverted_index_to_text(inverted_index: dict | None) -> str | None:
    """Convert OpenAlex abstract_inverted_index dict to continuous text, or return None if input is None."""
    if inverted_index is None:
        return None
    if not isinstance(inverted_index, dict):
        return "" # Return empty string for invalid format
        
    # Collect (position, term) tuples
    pos_terms: list[tuple[int, str]] = []
    for term, positions in inverted_index.items():
        if not isinstance(positions, list):
            continue # Skip invalid term entries
        for pos in positions:
            if isinstance(pos, int):
                 pos_terms.append((pos, term))
                 
    if not pos_terms:
        return "" # Return empty string if no valid terms/positions found
        
    pos_terms.sort(key=lambda x: x[0]) # Sort by position
    # Join terms based on sorted positions
    return " ".join(term for _, term in pos_terms)

# ----------------------------------------------------------------------
# Ingest JSON files & referenced works into DB
# ----------------------------------------------------------------------
def ingest_json_directory(input_dir: str | Path, db: DatabaseManager, email: str = "default@example.com"):
    """Read *.json files, insert each reference, gather all unique referenced_works IDs, fetch & insert them."""
    input_dir = Path(input_dir)
    if not input_dir.exists():
        print(f"[INGEST ERROR] Input dir {input_dir} does not exist")
        return

    print(f"[INGEST] Parsing JSON files in {input_dir} …")
    work_ids_to_fetch: set[str] = set()
    total_added, total_skipped = 0, 0
    processed_files_count = 0
    json_files_list = list(input_dir.glob("*.json"))
    total_files = len(json_files_list)
    print(f"[INGEST] Found {total_files} JSON files to process.")

    for i, jf in enumerate(json_files_list):
        # print(f"[INGEST] Processing file {i+1}/{total_files}: {jf.name}") # Verbose file progress
        file_added, file_skipped = 0, 0
        file_referenced_works = set()
        try:
            with open(jf, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
        except Exception as e:
            print(f"  [INGEST WARN] {jf.name}: unreadable JSON – {e}")
            continue

        refs: list[dict] = []
        if isinstance(data, list):
            refs = data
        elif isinstance(data, dict):
            # Handle common patterns: list under 'references', or dict of dicts
            if 'references' in data and isinstance(data['references'], list):
                refs = data['references']
            elif all(isinstance(v, dict) for v in data.values()):
                refs = list(data.values())
            else:
                refs = [data] # Assume the dict itself is the ref
        else:
            print(f"  [INGEST WARN] {jf.name}: unsupported JSON top-level type {type(data)}")
            continue
            
        refs = [r for r in refs if isinstance(r, dict)] # Ensure items are dicts

        for ref in refs:
            # Add filename and position to ref metadata if not present
            if 'filename' not in ref:
                ref['filename'] = jf.name
            # We don't have a reliable position within the file here, maybe skip adding?

            if db.add_reference(ref, table='input_references'): # Use specific table
                file_added += 1
            else:
                file_skipped += 1
                
            # Collect referenced works IDs from this reference
            for wid_url in (ref.get("referenced_works") or []):
                 if isinstance(wid_url, str) and wid_url.startswith('https://openalex.org/W'):
                    work_id_part = wid_url.split("/")[-1] # Extract Wxxxx
                    if work_id_part:
                         file_referenced_works.add(work_id_part)

        # Check collected referenced works against the DB *after* processing the file
        new_work_ids_from_file = set()
        for work_id in file_referenced_works:
            if not db.work_id_exists_in_db(work_id):
                new_work_ids_from_file.add(work_id)
            # else:
                # print(f"DEBUG: Referenced work {work_id} already in DB, skipping fetch.")

        if new_work_ids_from_file:
             work_ids_to_fetch.update(new_work_ids_from_file)
             # print(f"  [INGEST] {jf.name}: Added {len(new_work_ids_from_file)} new referenced works to fetch queue.")

        total_added += file_added
        total_skipped += file_skipped
        processed_files_count += 1
        # Optional: Print per-file summary
        # print(f"  [INGEST] {jf.name}: Added {file_added}, Skipped {file_skipped}. Found {len(file_referenced_works)} unique referenced works ({len(new_work_ids_from_file)} new). Total to fetch: {len(work_ids_to_fetch)}")

    print(f"[INGEST] Finished parsing {processed_files_count} files.")
    print(f"[INGEST] Total primary references: Added={total_added}, Skipped={total_skipped}.")
    print(f"[INGEST] Collected {len(work_ids_to_fetch)} unique referenced work IDs to fetch from OpenAlex.")

    if not work_ids_to_fetch:
        print("[INGEST] No new referenced works to fetch – OpenAlex step skipped.")
        return

    # Fetch their metadata via OpenAlex
    limiter = get_global_rate_limiter() # Use the global rate limiter shared across all components
    openalex_results = fetch_openalex_batch(list(work_ids_to_fetch), email=email, limiter=limiter)
    added_ref_works = 0
    
    print(f"[INGEST] Processing {len(openalex_results)} results from OpenAlex...")
    for oa_meta in openalex_results:
        # Convert OpenAlex result format to our reference dictionary format
        ref_to_add = {
            "id": oa_meta.get("id"),
            "doi": oa_meta.get("doi"),
            "title": oa_meta.get("display_name") or oa_meta.get("title"),
            "year": oa_meta.get("publication_year"),
            "authors": [a["author"].get("display_name") for a in oa_meta.get("authorships", []) if a.get("author") and a["author"].get("display_name")],
            "referenced_works": oa_meta.get("referenced_works"),
            "abstract": convert_inverted_index_to_text(oa_meta.get("abstract_inverted_index")),
            "source": "openalex_referenced_work" # Add source marker
        }
        # Add the fetched referenced work to the DB
        if db.add_reference(ref_to_add, table='input_references'): # Add to the same table for now
            added_ref_works += 1
            
    print(f"[INGEST] Inserted {added_ref_works} new referenced works from OpenAlex into the database.")
    # Commit AFTER adding all fetched works
    try:
        if added_ref_works > 0:
            print("[INGEST] Committing newly added referenced works to database...")
            db.conn.commit()
            print("[INGEST] Commit successful.")
    except sqlite3.Error as e:
        print(f"[INGEST ERROR] Failed to commit added referenced works: {e}")


# ----------------------------------------------------------------------
# Ingest a single BibTeX (.bib) file into DB
# ----------------------------------------------------------------------
def ingest_bib_file(bib_file_path: str | Path, db: DatabaseManager):
    """Read a .bib file, parse entries, and add them to the 'input_references' table.

    Args:
        bib_file_path: Path to the .bib file.
        db: Instance of ReferenceDB for database operations.
    """
    bib_file_path = Path(bib_file_path)
    if not bib_file_path.exists() or not bib_file_path.is_file():
        print(f"{RED}[INGEST ERROR] BibTeX file {bib_file_path} does not exist or is not a file.{RESET}")
        return

    print(f"{GREEN}[INGEST] Parsing BibTeX file: {bib_file_path.name} ...{RESET}")

    middlewares = []
    if LatexEncodingMiddleware and SeparateCoAuthors:
        middlewares = [
            LatexEncodingMiddleware(allow_inplace_modification=True),      # Converts LaTeX characters (e.g., \\"o) to unicode
            SeparateCoAuthors(allow_inplace_modification=True)             # Splits 'author' field by 'and'
        ]

    try:
        with open(bib_file_path, 'r', encoding='utf-8') as bibtex_file_handle:
            bib_content = bibtex_file_handle.read()
            # Handle potential Byte Order Mark (BOM)
            if bib_content.startswith('\ufeff'):
                bib_content = bib_content[1:]
            if middlewares:
                library = bibtexparser.loads(bib_content, append_middleware=middlewares)
            else:
                library = bibtexparser.loads(bib_content)
    except Exception as e: # Broadened for debugging
        print(f"  {RED}[INGEST DEBUG] Caught exception during parsing. Type: {type(e)}, Message: {e}{RESET}") # Debug print
        print(f"  {RED}[INGEST ERROR] Failed to parse {bib_file_path.name} (BibtexParserError): {e}{RESET}")
        return
    except Exception as e:
        print(f"  {RED}[INGEST ERROR] Failed to read or parse {bib_file_path.name}: {e}{RESET}")
        return

    total_added, total_skipped = 0, 0

    for i, entry in enumerate(library.entries):
        # Initialize ref_dict with all fields from the BibTeX entry, using original casing for keys.
        # Values are extracted from Field objects.
        ref_dict = {key: field_obj.value for key, field_obj in entry.fields_dict_preserve_case.items()}

        # Standardize key names for DB columns and specific processing.
        # This ensures that add_reference can find 'doi', 'title', etc., with lowercase keys.
        ref_dict['type'] = entry.entry_type.lower()
        ref_dict['bibtex_key'] = entry.key
        ref_dict['filename'] = bib_file_path.name
        ref_dict['position'] = i

        # Title
        title_val = entry.fields_dict.get('title', {}).get('value')
        if title_val is not None: ref_dict['title'] = title_val
        
        # Year & Publication Year
        year_val = entry.fields_dict.get('year', {}).get('value')
        if year_val is not None:
            ref_dict['year'] = year_val
            # Assume publication_year is the same as year unless a specific 'publication_year' field exists
            if entry.fields_dict.get('publication_year', {}).get('value') is None:
                 ref_dict['publication_year'] = year_val
            # else, if 'publication_year' was in bib, it's already in ref_dict from initial population

        # DOI
        doi_val = entry.fields_dict.get('doi', {}).get('value')
        if doi_val is not None: ref_dict['doi'] = doi_val

        # Abstract
        abstract_val = entry.fields_dict.get('abstract', {}).get('value')
        if abstract_val is not None: ref_dict['abstract'] = abstract_val

        # Container Title (e.g., journal, booktitle)
        container_val = (entry.fields_dict.get('journal', {}).get('value') or \
                         entry.fields_dict.get('booktitle', {}).get('value') or \
                         entry.fields_dict.get('series', {}).get('value'))
        if container_val is not None: ref_dict['container_title'] = container_val
        
        # OpenAlex ID (look for custom fields 'openalexid' or 'openalex_id')
        oa_id_val = entry.fields_dict.get('openalexid', {}).get('value') or \
                    entry.fields_dict.get('openalex_id', {}).get('value')
        if oa_id_val is not None: ref_dict['id'] = oa_id_val # 'id' is the key add_reference expects

        # Authors (special handling for list of Person objects or strings)
        authors_field_obj = entry.fields_dict.get('author')
        if authors_field_obj and authors_field_obj.value is not None:
            authors_value = authors_field_obj.value
            if isinstance(authors_value, list):
                # After SeparateCoAuthors middleware, authors_value should be a list of strings.
                ref_dict['authors'] = authors_value
            elif isinstance(authors_value, str): # Single author as a string (less common after middleware)
                ref_dict['authors'] = [authors_value] # Convert to list
        # If 'authors' is not set here, add_reference will get None for it.

        if db.add_reference(ref_dict, table='input_references'):
            total_added += 1
        else:
            total_skipped += 1
            
    print(f"{GREEN}[INGEST] Finished processing {bib_file_path.name}.{RESET}")
    print(f"{GREEN}[INGEST] Total entries: Added={total_added}, Skipped={total_skipped} (duplicates).{RESET}")

    if total_added > 0:
        try:
            print(f"{YELLOW}[INGEST] Committing {total_added} newly added BibTeX entries to database...{RESET}")
            db.conn.commit()
            print(f"{GREEN}[INGEST] Commit successful.{RESET}")
        except sqlite3.Error as e:
            print(f"{RED}[INGEST ERROR] Failed to commit added BibTeX entries: {e}{RESET}")

# ======================================================================
# Main Execution Block
# ======================================================================
if __name__ == "__main__":
    import argparse
    from pathlib import Path

    print(
        "[WARN] Running new_dl.py directly is legacy. Prefer 'python -m dl_lit.cli download-pdfs' for DB-first flows.",
        flush=True,
    )
    default_db_path = str((Path(__file__).resolve().parent.parent / "data" / "literature.db").resolve())

    parser = argparse.ArgumentParser(
        description="Ingest JSON references, fetch OpenAlex data, optionally download PDFs."
    )
    parser.add_argument(
        "--input", "--json-dir", dest="input_dir", 
        help="Directory containing input JSON reference files (e.g., from Zotero export)."
    )
    parser.add_argument(
        "--bib-file", dest="bib_file",
        help="Path to a single BibTeX (.bib) file to ingest."
    )
    parser.add_argument(
        "--output", "--output-dir", dest="output_dir", required=True,
        help="Directory to save downloaded PDFs and other outputs."
    )
    parser.add_argument(
        "--db-path", default=default_db_path,
        help="Path to the SQLite database file to use/create."
    )
    parser.add_argument(
        "--log-dir", default="logs",
        help="Directory to store log files."
    )
    parser.add_argument(
        "--legacy-metadata-dir",
        help="Optional: Directory containing older metadata JSON files to import into 'previous_references'."
    )
    parser.add_argument(
        "--email", default="default@example.com", # Replace with a real email if possible
        help="Email address for polite API usage (e.g., OpenAlex)."
    )
    parser.add_argument(
        "--threads", "--max-workers", dest="max_workers", type=int, default=5,
        help="Max concurrent download threads for the PDF download stage."
    )
    parser.add_argument(
        "--timeout", type=int, default=60,
        help="Timeout in seconds for network requests."
    )
    parser.add_argument(
        "--find-pdfs", action=argparse.BooleanOptionalAction, default=True,
        help="Enable/disable the PDF download stage after ingestion."
    )
    parser.add_argument(
        "--force-download", action="store_true",
        help="Force re-download of PDFs even if they already exist."
    )
    parser.add_argument(
        "--use-legacy-metadata", action="store_true", # Argument was used previously, keeping it for consistency although dir presence implies usage
        help="Flag to indicate use of legacy metadata (actual check uses directory presence)."
    )
    parser.add_argument(
        "--export-only", action=argparse.BooleanOptionalAction, default=False,
        help="If set, skip ingestion and download, only export BibTeX from 'current_references'."
    )

    args = parser.parse_args()

    # Ensure output directories exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path(args.log_dir).mkdir(parents=True, exist_ok=True)

    reference_db = None # Initialize to None for finally block
    try:
        # --- Database Setup and Initial Ingestion --- 
        reference_db = DatabaseManager(args.db_path)
        
        # Import legacy metadata if directory provided
        if args.legacy_metadata_dir:
            print("--- Running Legacy Metadata Import --- ")
            reference_db.import_legacy_metadata(args.legacy_metadata_dir)
            print("--- Finished Legacy Metadata Import --- ")
        else:
            print("[INFO] No legacy metadata directory provided, skipping import.")
        
        # --- Conditional Execution based on --export-only --- 
        if not args.export_only:
            ingestion_performed = False
            if args.bib_file:
                print(f"{YELLOW}--- Running BibTeX File Ingestion ({args.bib_file}) --- {RESET}")
                ingest_bib_file(args.bib_file, reference_db)
                print(f"{YELLOW}--- Finished BibTeX File Ingestion ({args.bib_file}) --- {RESET}")
                ingestion_performed = True
            elif args.input_dir:
                print(f"{YELLOW}--- Running JSON Ingestion ({args.input_dir}) --- {RESET}")
                ingest_json_directory(args.input_dir, reference_db, email=args.email)
                print(f"{YELLOW}--- Finished JSON Ingestion ({args.input_dir}) --- {RESET}")
                ingestion_performed = True
            
            if not ingestion_performed and not args.find_pdfs: # Only print if no action is taken in this block
                 print(f"{YELLOW}[INFO] No input JSON directory or BibTeX file provided for ingestion. Skipping ingestion phase.{RESET}")

            # --- PDF Download Stage --- 
            if args.find_pdfs:
                print("--- Running PDF Download Stage --- ")
                # Assuming BibliographyEnhancer uses the same db file
                enhancer = BibliographyEnhancer(args.db_path, email=args.email)
                processed_count = enhancer.process_references_from_database(
                    args.output_dir,
                    max_concurrent=args.max_workers,
                    force_download=args.force_download
                )
                print(f"--- Finished PDF Download Stage (Processed: {processed_count}) --- ")
                # Note: BibTeX export happens inside process_references_from_database in this case
            else:
                print("[INFO] PDF download stage skipped via --no-find-pdfs argument.")
                # Export BibTeX here if downloads were skipped but not in export-only mode
                print("--- Exporting BibTeX from 'current_references' (downloads skipped) --- ")
                enhancer = BibliographyEnhancer(args.db_path, email=args.email)
                enhancer.export_table_to_bibtex('current_references', args.output_dir)
                print("--- Finished BibTeX Export --- ")
        else:
            # --- Export Only Mode --- 
            print("--- Running in Export-Only Mode --- ")
            # Ensure enhancer is initialized for export
            enhancer = BibliographyEnhancer(args.db_path, email=args.email)
            print("--- Exporting BibTeX from 'current_references' --- ")
            enhancer.export_table_to_bibtex('current_references', args.output_dir)
            print("--- Finished BibTeX Export --- ")

    except Exception as e:
        print(f"[CRITICAL ERROR] An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
    finally:
        # Ensure database connection is always closed
        if reference_db:
            try:
                print("[INFO] Closing database connection.")
                reference_db.close_connection()
            except Exception as e:
                print(f"[ERROR] Failed to close database connection: {e}")

    print("--- Script Finished --- ")
