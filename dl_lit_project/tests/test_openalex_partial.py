"""Partial OpenAlex fetches are reported, quota exhaustion reaches the handlers that expect it,
and the LibGen error branches run (D30, D31; PY-47, PY-50, PY-51)."""
import importlib.util
import json
import sys
from pathlib import Path

import pytest
import requests

from dl_lit import OpenAlexScraper, new_dl, utils
from dl_lit.OpenAlexScraper import fetch_citing_work_ids, fetch_referenced_work_details
from dl_lit.utils import FetchResult, OpenAlexRateLimitExceeded, ServiceRateLimiter

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"


def _filter_ids(url):
    return url.split("openalex_id:", 1)[1].split("&", 1)[0].split("|")


def _work(wid):
    return {"id": f"https://openalex.org/{wid}", "display_name": wid, "authorships": []}


# --- OpenAlexScraper fetchers -----------------------------------------------------------------

def test_fetch_result_is_a_list_with_defaults():
    res = FetchResult([1, 2])
    assert res == [1, 2] and isinstance(res, list)
    assert res.complete is True and res.quota_exhausted is False
    assert res.errors == [] and res.failed_ids == []
    assert FetchResult([], complete=False, errors=["x"], failed_ids=["W1"]).failed_ids == ["W1"]


def test_citing_fetch_error_returns_incomplete(monkeypatch, capsys):
    calls = []

    def fake_request(**kwargs):
        calls.append(kwargs["url"])
        if len(calls) == 1:
            return {"results": [_work("W2"), _work("W3")], "meta": {"next_page": "https://api.openalex.org/works?page=2"}}
        raise requests.ConnectionError("connection reset")

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", fake_request)
    res = fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None)
    assert isinstance(res, FetchResult) and len(res) == 2 and res.complete is False and res.errors
    assert res.quota_exhausted is False
    assert "[OpenAlex WARN]" in capsys.readouterr().out


def test_citing_fetch_complete_prints_no_warning(monkeypatch, capsys):
    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", lambda **kw: {"results": [_work("W2")], "meta": {}})
    res = fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None)
    assert isinstance(res, FetchResult) and res.complete is True and len(res) == 1
    assert "[OpenAlex WARN]" not in capsys.readouterr().out


def test_quota_is_flagged_by_default_and_raised_on_request(monkeypatch):
    def quota(**kwargs):
        raise OpenAlexRateLimitExceeded("daily quota exhausted")

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", quota)
    res = fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None)
    assert isinstance(res, FetchResult) and res.quota_exhausted is True and res.complete is False
    assert res == []
    with pytest.raises(OpenAlexRateLimitExceeded):
        fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None, raise_on_quota=True)

    res = fetch_referenced_work_details(["W1"], None)
    assert isinstance(res, FetchResult) and res.quota_exhausted is True and res == []
    assert res.failed_ids == ["W1"]
    with pytest.raises(OpenAlexRateLimitExceeded):
        fetch_referenced_work_details(["W1"], None, raise_on_quota=True)


def test_referenced_batch_failure_lists_failed_ids(monkeypatch, capsys):
    def fake_request(**kwargs):
        ids = _filter_ids(kwargs["url"])
        if "W51" in ids:
            raise requests.ConnectionError("connection reset")
        return {"results": [_work(i) for i in ids]}

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", fake_request)
    ids = [f"W{i}" for i in range(1, 61)]
    res = fetch_referenced_work_details(ids, None)
    assert isinstance(res, FetchResult)
    assert res.failed_ids == [f"W{i}" for i in range(51, 61)]
    assert res.complete is False and len(res) == 50 and res.errors
    assert "[OpenAlex WARN]" in capsys.readouterr().out


# --- new_dl.fetch_openalex_batch --------------------------------------------------------------

class _Resp:
    def __init__(self, ids):
        self._ids = ids

    def raise_for_status(self):
        return None

    def json(self):
        return {"results": [_work(i) for i in self._ids]}


class _Session:
    def get(self, url, **kwargs):
        ids = _filter_ids(url)
        if "W51" in ids:
            raise requests.Timeout("read timed out")
        return _Resp(ids)


def test_fetch_openalex_batch_with_the_real_limiter(monkeypatch, capsys):
    limiter = ServiceRateLimiter({"default": {"limit": 1000, "window": 1}, "openalex": {"limit": 1000, "window": 1}})
    monkeypatch.setattr(new_dl, "get_openalex_session", lambda: _Session())
    ids = [f"W{i}" for i in range(1, 61)]
    res = new_dl.fetch_openalex_batch(ids, limiter=limiter)
    assert isinstance(res, FetchResult)
    assert res.failed_ids == [f"W{i}" for i in range(51, 61)]
    assert res.complete is False and len(res) == 50
    assert "[OpenAlex WARN]" in capsys.readouterr().out


# --- LibGen error branches (D31) --------------------------------------------------------------

class _NoLimit:
    def wait_if_needed(self, *a, **k):
        return True


def _libgen_error(monkeypatch, status):
    enhancer = new_dl.BibliographyEnhancer(db_manager=None, rate_limiter=_NoLimit(), llm_optional=True)
    resp = requests.Response()
    resp.status_code = status
    resp._content = b"<html>busy</html>"
    resp.url = "https://libgen.li/index.php"

    def fail(*a, **k):
        raise requests.HTTPError(f"{status} error", response=resp)

    monkeypatch.setattr(enhancer, "_request_get_direct", fail)
    captured = []
    monkeypatch.setattr(enhancer, "_save_libgen_debug_response", lambda **kw: captured.append(kw))
    assert enhancer.search_libgen("Some Title", ["Author"]) == []
    return captured


def test_libgen_429_saves_debug_capture(monkeypatch):
    captured = _libgen_error(monkeypatch, 429)
    assert [c["context"] for c in captured] == ["http_429", "http_429"]


def test_libgen_503_saves_debug_capture(monkeypatch):
    captured = _libgen_error(monkeypatch, 503)
    assert [c["context"] for c in captured] == ["http_503", "http_503"]


# --- quota exhaustion stops keyword and seed expansion (D30) ----------------------------------

def _load_script(monkeypatch, name, module_name):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS_DIR / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _quota_fetcher(recorded, result):
    def fetch(*args, **kwargs):
        recorded.append(kwargs)
        if kwargs.get("raise_on_quota"):
            raise OpenAlexRateLimitExceeded("daily quota exhausted")
        return result
    return fetch


def test_keyword_expansion_stops_on_quota(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    recorded = []
    monkeypatch.setattr(mod, "fetch_citing_work_ids", _quota_fetcher(recorded, FetchResult()))
    detail_calls = []
    monkeypatch.setattr(mod, "fetch_referenced_work_details", lambda *a, **k: detail_calls.append(k) or [])
    base = [{"id": "https://openalex.org/W1", "display_name": "one"}]
    stats = mod._crawl_direction(base, 2, 10, None, "upstream", [], set(), None)
    assert "Expansion stopped early" in capsys.readouterr().out
    assert recorded and recorded[0].get("raise_on_quota") is True
    assert detail_calls == []
    assert stats["processed"] == 1


def test_keyword_detail_fetch_stops_on_quota(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    recorded = []
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_fetcher(recorded, FetchResult()))
    base = [{"id": "https://openalex.org/W1", "display_name": "one", "referenced_works": ["https://openalex.org/W2"]}]
    mod._crawl_direction(base, 2, 10, None, "downstream", [], set(), None)
    assert "Expansion detail fetch stopped early" in capsys.readouterr().out
    assert recorded and recorded[0].get("raise_on_quota") is True


def _seed_expand_main(monkeypatch, tmp_path, mod, *direction_flags):
    work = {"id": "https://openalex.org/W1", "display_name": "one", "referenced_works": ["https://openalex.org/W2"]}
    monkeypatch.setattr(mod, "fetch_work_by_openalex_id", lambda *a, **k: dict(work))
    monkeypatch.setattr(
        sys,
        "argv",
        ["seed_expand", "--db-path", str(tmp_path / "t.db"), "--seed-json", json.dumps([{"openalex_id": "W1", "title": "one"}]), *direction_flags],
    )
    mod.main()


def test_seed_expand_upstream_quota_passes_through_collect_for_direction(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    keyword_search = sys.modules["keyword_search"]
    recorded = []
    monkeypatch.setattr(keyword_search, "fetch_citing_work_ids", _quota_fetcher(recorded, FetchResult()))
    _seed_expand_main(monkeypatch, tmp_path, mod, "--include-upstream")
    captured = capsys.readouterr()
    assert "upstream expansion stopped early" in captured.err
    assert recorded and recorded[0].get("raise_on_quota") is True
    assert json.loads(captured.out.strip().splitlines()[-1]) == {"runs": []}


def test_seed_expand_detail_fetch_stops_on_quota(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    recorded = []
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_fetcher(recorded, FetchResult()))
    _seed_expand_main(monkeypatch, tmp_path, mod, "--include-downstream")
    captured = capsys.readouterr()
    assert "detail fetch stopped early" in captured.err
    assert recorded and recorded[0].get("raise_on_quota") is True
