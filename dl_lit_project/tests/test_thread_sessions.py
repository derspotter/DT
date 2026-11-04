"""Thread-local keep-alive sessions and the landing-page single fetch."""
import threading
from collections import Counter

import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from dl_lit import new_dl, utils
from dl_lit.pdf_downloader import PDFDownloader

PDF_BYTES = b"%PDF-1.4 fake pdf body"


@pytest.fixture(autouse=True)
def _fresh_session_cache(monkeypatch):
    # Each test starts with an empty thread-local cache so mounted fake adapters never leak.
    monkeypatch.setattr(utils, "_thread_sessions_local", threading.local())


class _FakeTransport(BaseAdapter):
    """Serves canned responses per URL and counts every request it sees."""

    def __init__(self, routes):
        super().__init__()
        self.routes = routes
        self.calls = Counter()

    def send(self, request, **kwargs):
        self.calls[request.url] += 1
        status, content_type, body = self.routes[request.url]
        response = requests.Response()
        response.status_code = status
        response.headers = CaseInsensitiveDict({"Content-Type": content_type})
        response._content = body
        response.url = request.url
        response.request = request
        response.encoding = "utf-8"
        return response

    def close(self):
        pass


# --- get_thread_session -----------------------------------------------------

def test_same_session_on_repeated_calls_in_one_thread():
    assert utils.get_thread_session("download") is utils.get_thread_session("download")


def test_different_session_in_another_thread():
    mine = utils.get_thread_session("download")
    seen = []
    t = threading.Thread(target=lambda: seen.append(utils.get_thread_session("download")))
    t.start()
    t.join()
    assert seen and seen[0] is not mine


def test_different_session_per_kind_and_proxies():
    proxies = {"http": "http://proxy:3128", "https": "http://proxy:3128"}
    plain = utils.get_thread_session("download")
    vpn = utils.get_thread_session("download-vpn", proxies=proxies)
    other_proxy = utils.get_thread_session("download-vpn", proxies={"https": "http://other:1"})
    assert plain is not vpn
    assert vpn is not other_proxy
    assert utils.get_thread_session("download-vpn", proxies=dict(reversed(list(proxies.items())))) is vpn
    assert vpn.proxies["https"] == "http://proxy:3128"
    assert plain.proxies == {}


def test_trust_env_false_by_default_and_pool_size_applied():
    session = utils.get_thread_session("download", pool_maxsize=7)
    assert session.trust_env is False
    adapter = session.get_adapter("https://example.org/")
    assert adapter._pool_maxsize == 7
    assert session.get_adapter("http://example.org/")._pool_maxsize == 7


def test_openalex_session_is_the_openalex_thread_session():
    session = utils.get_openalex_session()
    assert session is utils.get_thread_session("openalex", pool_maxsize=64)
    assert session.get_adapter("https://api.openalex.org/")._pool_maxsize == 64


# --- download_paper landing page --------------------------------------------

def test_download_paper_parses_the_landing_page_it_already_fetched(tmp_path, monkeypatch):
    monkeypatch.delenv("RAG_FEEDER_VPN_PROXY_URL", raising=False)
    monkeypatch.setattr(PDFDownloader, "validate_pdf", lambda self, content, ref_type=None: content.startswith(b"%PDF-"))
    landing = "https://publisher.example/article/1"
    pdf = "https://publisher.example/files/paper.pdf"
    html = b'<html><body><a href="/files/paper.pdf">Download PDF</a></body></html>'
    transport = _FakeTransport({
        landing: (200, "text/html; charset=utf-8", html),
        pdf: (200, "application/pdf", PDF_BYTES),
    })
    session = utils.get_thread_session("download")
    session.mount("https://", transport)

    enhancer = new_dl.BibliographyEnhancer(db_manager=None, rate_limiter=None, llm_optional=True)
    parsed = []
    real_extract = enhancer._extract_pdf_link_from_response

    def _spy(response):
        parsed.append(response.url)
        return real_extract(response)

    monkeypatch.setattr(enhancer, "_extract_pdf_link_from_response", _spy)

    target = tmp_path / "out.pdf"
    assert enhancer.download_paper(landing, str(target), "article") is True

    assert transport.calls[landing] == 1  # no second GET of the landing page
    assert transport.calls[pdf] == 1
    assert parsed == [landing]
    assert target.read_bytes() == PDF_BYTES


# --- fix round 1 ------------------------------------------------------------

def test_openalex_session_honours_the_environment_but_downloads_do_not(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://corp-proxy:3128")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/etc/ssl/certs/corp.pem")
    assert utils.get_openalex_session().trust_env is True
    assert utils.get_thread_session("download").trust_env is False


def test_searcher_openalex_session_has_no_urllib3_retry_crossref_and_s2_keep_theirs():
    from dl_lit.OpenAlexScraper import OpenAlexCrossrefSearcher

    searcher = OpenAlexCrossrefSearcher(rate_limiter=object())
    for scheme in ("https://", "http://"):
        adapter = searcher.openalex_session.get_adapter(scheme + "api.openalex.org/")
        assert adapter.max_retries.total == 0
        assert adapter._pool_maxsize == 64
        assert adapter._pool_connections == 64
    for session in (searcher.crossref_session, searcher.semantic_scholar_session):
        retry = session.get_adapter("https://api.crossref.org/").max_retries
        assert retry.total == 5
        assert 429 in retry.status_forcelist


# --- second pass: the routes really use their sessions ---------------------------------------

class _RecordingTransport(_FakeTransport):
    """Also records the proxies each request is sent with."""

    def __init__(self, routes):
        super().__init__(routes)
        self.proxies = []

    def send(self, request, **kwargs):
        self.proxies.append(kwargs.get("proxies"))
        return super().send(request, **kwargs)


def test_vpn_route_sends_through_the_proxy(monkeypatch):
    url = "http://example.test/paper.pdf"
    proxies = {"http": "http://proxy.test:3128", "https": "http://proxy.test:3128"}
    transport = _RecordingTransport({url: (200, "application/pdf", b"%PDF-")})
    enhancer = new_dl.BibliographyEnhancer.__new__(new_dl.BibliographyEnhancer)
    enhancer.proxies = proxies
    monkeypatch.setattr(enhancer, "_is_eduvpn_connected", lambda *a, **k: True)
    utils.get_thread_session("download-vpn", proxies=proxies).mount("http://", transport)

    enhancer._request_get_vpn(url, timeout=1)

    assert transport.calls[url] == 1
    assert transport.proxies[0]["http"] == proxies["http"]


def test_pdf_downloader_downloads_through_the_thread_session(tmp_path, monkeypatch):
    url = "http://example.test/paper.pdf"
    transport = _RecordingTransport({url: (200, "application/pdf", b"%PDF-1.4 body")})
    utils.get_thread_session("download").mount("http://", transport)
    monkeypatch.setattr(PDFDownloader, "validate_pdf", lambda self, content, ref_type=None: True)
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("bare requests.get"))
    limiter = type("Limiter", (), {"wait_if_needed": lambda self, service: None})()
    downloader = PDFDownloader(tmp_path, limiter)

    path, checksum, error, _ = downloader._download_and_save(url, "paper.pdf", "direct")

    assert error is None and path is not None and checksum
    assert transport.calls[url] == 1
