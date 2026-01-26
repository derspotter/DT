import pytest

from dl_lit.keyword_search import normalize_query, QuerySyntaxError, openalex_result_to_record


def test_normalize_query_inserts_and():
    assert normalize_query("foo bar") == "foo AND bar"


def test_normalize_query_parentheses():
    assert normalize_query("(foo OR bar) AND baz") == "( foo OR bar ) AND baz".replace("  ", " ")


def test_normalize_query_mismatched_parens():
    with pytest.raises(QuerySyntaxError):
        normalize_query("(foo AND bar")


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
