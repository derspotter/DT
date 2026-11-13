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
    payload = json.loads(captured.out.strip().splitlines()[-1])
    assert payload["runs"] == [] and payload["quota_exhausted"] is True and "quota" in payload["message"]


def test_seed_expand_detail_fetch_stops_on_quota(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    recorded = []
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_fetcher(recorded, FetchResult()))
    _seed_expand_main(monkeypatch, tmp_path, mod, "--include-downstream")
    captured = capsys.readouterr()
    assert "detail fetch stopped early" in captured.err
    assert recorded and recorded[0].get("raise_on_quota") is True


class _QuotaSession:
    def get(self, url, **kwargs):
        ids = _filter_ids(url)
        if "W51" in ids:
            raise OpenAlexRateLimitExceeded("daily quota exhausted")
        return _Resp(ids)


def test_fetch_openalex_batch_maps_quota_exhaustion_to_an_incomplete_result(monkeypatch, capsys):
    limiter = ServiceRateLimiter({"default": {"limit": 1000, "window": 1}, "openalex": {"limit": 1000, "window": 1}})
    monkeypatch.setattr(new_dl, "get_openalex_session", lambda: _QuotaSession())
    ids = [f"W{i}" for i in range(1, 121)]
    res = new_dl.fetch_openalex_batch(ids, limiter=limiter)
    assert isinstance(res, FetchResult)
    assert res.complete is False and res.quota_exhausted is True
    assert res.errors and "daily quota exhausted" in res.errors[0]
    assert res.failed_ids == [f"W{i}" for i in range(51, 121)]  # quota stops the fetch: batches 2 and 3
    assert len(res) == 50
    assert "[OpenAlex WARN] referenced works incomplete" in capsys.readouterr().out


# --- final fix wave S-I1: a quota stop keeps what was fetched and is reported -----------------

def test_quota_exception_carries_the_partial_results(monkeypatch):
    def fake_request(**kwargs):
        url = kwargs["url"]
        if "openalex_id:" in url:
            ids = _filter_ids(url)
            if "W51" in ids:
                raise OpenAlexRateLimitExceeded("daily quota exhausted")
            return {"results": [_work(i) for i in ids]}
        if "page2" in url:
            raise OpenAlexRateLimitExceeded("daily quota exhausted")
        return {"results": [_work("C1"), _work("C2")], "meta": {"next_page": "https://api.openalex.org/page2"}}

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", fake_request)
    with pytest.raises(OpenAlexRateLimitExceeded) as citing:
        fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None, raise_on_quota=True)
    assert [w["openalex_id"] for w in citing.value.partial] == ["https://openalex.org/C1", "https://openalex.org/C2"]
    assert citing.value.partial.quota_exhausted is True

    with pytest.raises(OpenAlexRateLimitExceeded) as details:
        fetch_referenced_work_details([f"W{i}" for i in range(1, 61)], None, raise_on_quota=True)
    assert len(details.value.partial) == 50 and details.value.partial.failed_ids == [f"W{i}" for i in range(51, 61)]


def _quota_with_partial(recorded, partial):
    def fetch(*args, **kwargs):
        recorded.append(kwargs)
        exc = OpenAlexRateLimitExceeded("daily quota exhausted")
        exc.partial = FetchResult(partial, complete=False, quota_exhausted=True)
        raise exc
    return fetch


def test_keyword_detail_quota_keeps_partial_details_and_stops_the_crawl(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    recorded = []
    monkeypatch.setattr(
        mod, "fetch_referenced_work_details", _quota_with_partial(recorded, [{"openalex_id": "https://openalex.org/W2", "title": "two"}])
    )
    collect_calls = []
    real_collect = mod._collect_candidate_ids
    monkeypatch.setattr(mod, "_collect_candidate_ids", lambda *a, **k: collect_calls.append(1) or real_collect(*a, **k))
    base = [{"id": "https://openalex.org/W1", "display_name": "one", "referenced_works": ["https://openalex.org/W2", "https://openalex.org/W3"]}]
    all_items = list(base)
    stats = mod._crawl_direction(base, 3, 10, None, "downstream", all_items, set(), None)
    assert stats["quota_exhausted"] is True and stats["added"] == 2
    # W2's details are kept; W3, whose details never came, is kept as its bare id (round 2).
    assert [i["id"] for i in all_items] == ["https://openalex.org/W1", "https://openalex.org/W2", "https://openalex.org/W3"]
    assert all_items[1]["display_name"] == "two" and all_items[2]["display_name"] is None
    assert len(collect_calls) == 1  # kept but not crawled further


def test_keyword_citing_quota_keeps_the_partial_candidates(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    monkeypatch.setattr(
        mod, "fetch_citing_work_ids", _quota_with_partial([], [{"openalex_id": "https://openalex.org/W5"}])
    )
    asked = []
    monkeypatch.setattr(
        mod,
        "fetch_referenced_work_details",
        lambda ids, *a, **k: asked.append(list(ids)) or FetchResult([{"openalex_id": "https://openalex.org/W5", "title": "c"}]),
    )
    base = [{"id": "https://openalex.org/W1", "display_name": "one"}]
    all_items = list(base)
    stats = mod._crawl_direction(base, 2, 10, None, "upstream", all_items, set(), None)
    assert stats["quota_exhausted"] is True and stats["added"] == 1
    assert asked and "W5" in asked[0][0]


def test_keyword_expansion_reports_the_quota_stop_and_skips_the_next_direction(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    directions = []

    def crawl(**kwargs):
        directions.append(kwargs["direction"])
        return {"added": 1, "processed": 1, "matched": 1, "quota_exhausted": True}

    monkeypatch.setattr(mod, "_crawl_direction", crawl)
    base = [{"id": "https://openalex.org/W1", "display_name": "one"}]
    _, stats = mod.expand_references_recursive(base, 1, 1, 10, None, include_downstream=True, include_upstream=True)
    assert directions == ["downstream"]
    assert stats["quota_exhausted"] is True and stats["message"] == mod.QUOTA_STOP_MESSAGE


def test_keyword_search_payload_and_run_report_the_quota_stop(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    items = [{"id": "https://openalex.org/W1", "display_name": "one"}]

    def fake_search(**kwargs):
        kwargs["on_page"](items, {"count": 1})
        return items

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(
        mod,
        "expand_references_recursive",
        lambda base_items, **kwargs: (
            list(base_items) + [{"id": "https://openalex.org/W2", "display_name": "two"}],
            {"added": 1, "processed": 1, "downstream_added": 1, "upstream_added": 0, "quota_exhausted": True, "message": mod.QUOTA_STOP_MESSAGE},
        ),
    )
    monkeypatch.setattr(sys, "argv", [
        "x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1", "--include-downstream", "--related-depth", "1",
    ])
    mod.main()
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines() if line.startswith("{")]
    final = lines[-1]
    assert final["quota_exhausted"] is True and final["message"] == mod.QUOTA_STOP_MESSAGE
    assert final["fetched_count"] == 2
    from dl_lit.db_manager import DatabaseManager

    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, error FROM search_runs WHERE id = ?", (final["runId"],)).fetchone()
    db.close_connection()
    assert tuple(row) == ("done", mod.QUOTA_STOP_MESSAGE)


def test_seed_expand_keeps_partial_details_reports_and_stops(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    recorded = []
    monkeypatch.setattr(
        mod, "fetch_referenced_work_details", _quota_with_partial(recorded, [{"openalex_id": "https://openalex.org/W2", "title": "two"}])
    )
    work = {"id": "https://openalex.org/W1", "display_name": "one", "referenced_works": ["https://openalex.org/W2"]}
    monkeypatch.setattr(mod, "fetch_work_by_openalex_id", lambda *a, **k: dict(work))
    seeds = [{"openalex_id": "W1", "title": "one"}, {"openalex_id": "W9", "title": "nine"}]
    monkeypatch.setattr(
        sys, "argv",
        ["seed_expand", "--db-path", str(tmp_path / "t.db"), "--seed-json", json.dumps(seeds), "--include-downstream", "--include-upstream"],
    )
    mod.main()
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["quota_exhausted"] is True and payload["message"] == mod.QUOTA_STOP_MESSAGE
    assert [(r["direction"], r["count"]) for r in payload["runs"]] == [("downstream", 1)]
    assert len(recorded) == 1  # no upstream direction, no second seed after the stop


# --- round 2: partial citing results survive a second quota hit on the detail fetch ------------

def test_keyword_citing_partial_survives_a_detail_quota(monkeypatch, capsys):
    mod = _load_script(monkeypatch, "keyword_search", "keyword_search_partial_test")
    monkeypatch.setattr(
        mod, "fetch_citing_work_ids",
        _quota_with_partial([], [{"openalex_id": "https://openalex.org/W5", "title": "Citing Five", "year": 2020}]),
    )
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_with_partial([], []))
    base = [{"id": "https://openalex.org/W1", "display_name": "one"}]
    all_items = list(base)
    stats = mod._crawl_direction(base, 2, 10, None, "upstream", all_items, set(), None)
    assert stats["quota_exhausted"] is True and stats["added"] == 1
    assert [(i["id"], i["display_name"], i["publication_year"]) for i in all_items[1:]] == [
        ("https://openalex.org/W5", "Citing Five", 2020)
    ]
    record = mod.openalex_result_to_record(all_items[1], run_id=1)
    assert record["openalex_id"] and record["title"] == "Citing Five"


def test_seed_expand_citing_partial_survives_a_detail_quota(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    keyword_search = sys.modules["keyword_search"]
    monkeypatch.setattr(
        keyword_search, "fetch_citing_work_ids",
        _quota_with_partial([], [{"openalex_id": "https://openalex.org/W5", "title": "Citing Five", "year": 2020}]),
    )
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_with_partial([], []))
    _seed_expand_main(monkeypatch, tmp_path, mod, "--include-upstream")
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["quota_exhausted"] is True
    assert [(r["direction"], r["count"]) for r in payload["runs"]] == [("upstream", 1)]
    from dl_lit.db_manager import DatabaseManager

    db = DatabaseManager(db_path=tmp_path / "t.db")
    rows = db.conn.execute("SELECT openalex_id, title FROM search_results WHERE search_run_id = ?", (payload["runs"][0]["run_id"],)).fetchall()
    db.close_connection()
    assert [tuple(r) for r in rows] == [("https://openalex.org/W5", "Citing Five")]


def test_seed_expand_keeps_bare_ids_when_the_detail_fetch_hits_quota(monkeypatch, tmp_path, capsys):
    mod = _load_script(monkeypatch, "seed_expand", "seed_expand_partial_test")
    monkeypatch.setattr(mod, "fetch_referenced_work_details", _quota_with_partial([], []))
    _seed_expand_main(monkeypatch, tmp_path, mod, "--include-downstream")
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["quota_exhausted"] is True
    assert [(r["direction"], r["count"]) for r in payload["runs"]] == [("downstream", 1)]
