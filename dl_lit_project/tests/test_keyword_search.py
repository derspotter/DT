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

    def fake_request(params, rate_limiter, retries: int = 3):
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
    assert "publication_year:>=2020" in captured["filter"]
    assert "publication_year:<=2021" in captured["filter"]
