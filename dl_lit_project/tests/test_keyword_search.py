import pytest

from dl_lit import keyword_search
from dl_lit.keyword_search import (
    QuerySyntaxError,
    build_openalex_query_text,
    normalize_query,
    openalex_result_to_record,
)


def test_normalize_query_inserts_and():
    assert normalize_query("foo bar") == "foo AND bar"


def test_normalize_query_parentheses():
    assert normalize_query("(foo OR bar) AND baz") == "( foo OR bar ) AND baz".replace("  ", " ")


def test_normalize_query_mismatched_parens():
    with pytest.raises(QuerySyntaxError):
        normalize_query("(foo AND bar")


def test_build_openalex_query_text_strips_boolean_syntax():
    assert build_openalex_query_text('(foo OR bar) AND "baz qux"') == "foo bar baz qux"


def test_build_openalex_query_text_strips_apostrophes():
    assert build_openalex_query_text("baumol's disease") == "baumol s disease"


def test_openalex_result_to_record_pages():
    item = {
        "id": "https://openalex.org/W1",
        "display_name": "Test",
        "publication_year": 2020,
        "authorships": [],
        "biblio": {"first_page": "1", "last_page": "10"},
        "primary_location": {"source": {"display_name": "Journal"}},
    }
    record = openalex_result_to_record(item, run_id=1)
    assert record["pages"] == "1--10"
    assert record["run_id"] == 1


def test_search_openalex_field_filter(monkeypatch):
    captured = {}

    class DummyLimiter:
        def wait_if_needed(self, *_args, **_kwargs):
            return None

    def fake_request(endpoint, params, rate_limiter, retries: int = 3):
        assert endpoint == "works"
        captured.update(params)
        return {"results": [], "meta": {}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    monkeypatch.setattr(keyword_search, "get_global_rate_limiter", lambda: DummyLimiter())

    keyword_search.search_openalex(
        "foo AND bar",
        max_results=1,
        year_from=2020,
        year_to=2021,
        field="title",
        mailto="test@example.com",
    )

    assert "search" not in captured
    assert captured["filter"].startswith("title.search:foo bar")
    assert "publication_year:2020-2021" in captured["filter"]


def test_search_openalex_author_only_filter(monkeypatch):
    captured = {}

    class DummyLimiter:
        def wait_if_needed(self, *_args, **_kwargs):
            return None

    def fake_request(endpoint, params, rate_limiter, retries: int = 3):
        if endpoint == "authors":
            return {"results": [{"id": "https://openalex.org/A123", "display_name": "Elinor Ostrom"}]}
        assert endpoint == "works"
        captured.update(params)
        return {"results": [], "meta": {}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    monkeypatch.setattr(keyword_search, "get_global_rate_limiter", lambda: DummyLimiter())

    keyword_search.search_openalex(
        "",
        max_results=1,
        author="Elinor Ostrom",
    )

    assert "search" not in captured
    assert captured["filter"] == "authorships.author.id:A123"


def test_search_openalex_uncapped_pages_until_cursor_exhausted(monkeypatch):
    class DummyLimiter:
        def wait_if_needed(self, *_args, **_kwargs):
            return None

    pages = [
        {"results": [{"id": f"W{i}"} for i in range(200)], "meta": {"next_cursor": "c2"}},
        {"results": [{"id": f"W{200 + i}"} for i in range(50)], "meta": {"next_cursor": None}},
    ]
    calls = []

    def fake_request(endpoint, params, rate_limiter, retries: int = 3):
        assert endpoint == "works"
        calls.append(dict(params))
        return pages[len(calls) - 1]

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    monkeypatch.setattr(keyword_search, "get_global_rate_limiter", lambda: DummyLimiter())

    results = keyword_search.search_openalex("foo", max_results=None)

    assert len(results) == 250
    assert len(calls) == 2


def test_search_openalex_sort_param(monkeypatch):
    captured = {}

    class DummyLimiter:
        def wait_if_needed(self, *_args, **_kwargs):
            return None

    def fake_request(endpoint, params, rate_limiter, retries: int = 3):
        captured.update(params)
        return {"results": [], "meta": {}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    monkeypatch.setattr(keyword_search, "get_global_rate_limiter", lambda: DummyLimiter())

    keyword_search.search_openalex("foo", max_results=1, sort="cited_by_count")

    assert captured["sort"] == "cited_by_count:desc"


def test_search_openalex_relevance_sort_requires_search_text(monkeypatch):
    captured = {}

    class DummyLimiter:
        def wait_if_needed(self, *_args, **_kwargs):
            return None

    def fake_request(endpoint, params, rate_limiter, retries: int = 3):
        if endpoint == "authors":
            return {"results": [{"id": "https://openalex.org/A123", "display_name": "Elinor Ostrom"}]}
        captured.update(params)
        return {"results": [], "meta": {}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    monkeypatch.setattr(keyword_search, "get_global_rate_limiter", lambda: DummyLimiter())

    keyword_search.search_openalex("", max_results=1, author="Elinor Ostrom", sort="relevance")

    assert "sort" not in captured


def test_search_openalex_rejects_unknown_sort():
    with pytest.raises(ValueError):
        keyword_search.search_openalex("foo", sort="banana")


def test_search_select_includes_reference_counts(monkeypatch):
    captured = {}

    def fake_request(endpoint, params, rate_limiter, retries=3):
        captured.update(params)
        return {"results": [], "meta": {"next_cursor": None}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    keyword_search.search_openalex(query="labour", max_results=5)
    select_fields = captured["select"].split(",")
    assert "referenced_works_count" in select_fields
    assert "cited_by_count" in select_fields


def test_effective_max_results():
    assert keyword_search.effective_max_results(0) is None
    assert keyword_search.effective_max_results(-5) is None
    assert keyword_search.effective_max_results(None) is None
    assert keyword_search.effective_max_results("") is None
    assert keyword_search.effective_max_results(25) == 25
    assert keyword_search.effective_max_results("200") == 200
