"""Every former identifier-helper copy keeps its outputs after the move to dl_lit.identifiers.

``fixtures/identifier_cases.json`` was recorded from the OLD functions (commit
8c3e4d4, code at 5a776e0).  For each former copy the test checks both the
shared function it now maps to and the name its call sites still use.
"""

import ast
import importlib.util
import json
from pathlib import Path

import pytest

from dl_lit import identifiers
from dl_lit.identifiers import (
    extract_doi_lower,
    inverted_index_to_text,
    inverted_index_to_text_lenient,
    normalize_doi,
    normalize_doi_lower,
    normalize_openalex_id,
    normalize_openalex_id_strict,
    openalex_work_to_record,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "identifier_cases.json").read_text())
SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"
RAISES = "__raises__"


def _script(name):
    import sys

    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(f"{name}_identifiers_test", SCRIPTS_DIR / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def db():
    from dl_lit.db_manager import DatabaseManager

    manager = DatabaseManager(":memory:")
    yield manager
    manager.close_connection()


# former copy -> (shared function or None when kept private, getter for the name call sites use)
DOI_MAP = {
    "dl_lit.db_manager.DatabaseManager._normalize_doi": (normalize_doi, lambda db: db._normalize_doi),
    "backend.scripts.ingest_latest.normalize_doi": (normalize_doi, lambda db: _script("ingest_latest").normalize_doi),
    # graph_3d_export / migrate_to_works trimmed the input first: normalize_doi(str(v).strip()).
    "backend.scripts.graph_3d_export.normalize_doi": (None, lambda db: _script("graph_3d_export").normalize_doi),
    "backend.scripts.migrate_to_works.normalize_doi": (None, lambda db: _script("migrate_to_works").normalize_doi),
    "dl_lit.OpenAlexScraper.normalize_doi": (normalize_doi_lower, lambda db: __import__("dl_lit.OpenAlexScraper", fromlist=["x"]).normalize_doi),
    "backend.scripts.keyword_search.normalize_doi": (extract_doi_lower, lambda db: _script("keyword_search").normalize_doi),
    # Kept private: they match none of the shared rules (see test_private_doi_copies_differ).
    "backend.scripts.backfill_openalex_topics.normalize_doi": (None, lambda db: _script("backfill_openalex_topics")._normalize_doi),
    "backend.scripts.export_bundle.normalize_doi": (None, lambda db: _script("export_bundle")._normalize_doi),
    "backend.scripts.upstream_update.normalize_doi": (None, lambda db: _script("upstream_update")._normalize_doi),
}

OPENALEX_MAP = {
    "dl_lit.db_manager.DatabaseManager._normalize_openalex_id": (normalize_openalex_id, lambda db: db._normalize_openalex_id),
    "backend.scripts.backfill_openalex_topics.normalize_openalex_id": (normalize_openalex_id, lambda db: _script("backfill_openalex_topics").normalize_openalex_id),
    "backend.scripts.backfill_citation_edges.normalize_openalex_id": (normalize_openalex_id, lambda db: _script("backfill_citation_edges").normalize_openalex_id),
    "backend.scripts.migrate_to_works.normalize_openalex_id": (normalize_openalex_id, lambda db: _script("migrate_to_works").normalize_openalex_id),
    "backend.scripts.graph_3d_export.normalize_openalex_id": (normalize_openalex_id, lambda db: _script("graph_3d_export").normalize_openalex_id),
    "backend.scripts.keyword_search.normalize_openalex_id": (normalize_openalex_id_strict, lambda db: _script("keyword_search").normalize_openalex_id),
    # Kept private: returns the last URL segment / the input unchanged (no case, no W-check).
    "dl_lit.OpenAlexScraper._normalize_openalex_id": (None, lambda db: __import__("dl_lit.OpenAlexScraper", fromlist=["x"])._normalize_openalex_id),
}

INVERTED_MAP = {
    "dl_lit.db_manager.DatabaseManager._convert_inverted_index_to_text": (inverted_index_to_text, lambda db: db._convert_inverted_index_to_text),
    "dl_lit.OpenAlexScraper.get_abstract": (inverted_index_to_text, lambda db: __import__("dl_lit.OpenAlexScraper", fromlist=["x"]).get_abstract),
    # Returned "" instead of None for empty input: now ``inverted_index_to_text(idx) or ""``.
    "dl_lit.OpenAlexScraper.convert_inverted_index_to_text": (None, lambda db: __import__("dl_lit.OpenAlexScraper", fromlist=["x"]).convert_inverted_index_to_text),
    "dl_lit.new_dl.convert_inverted_index_to_text": (inverted_index_to_text_lenient, lambda db: __import__("dl_lit.new_dl", fromlist=["x"]).convert_inverted_index_to_text),
}


def _cases(category, mapping):
    for name, (shared, getter) in mapping.items():
        for raw_input, expected in FIXTURE[category][name].items():
            yield pytest.param(name, shared, getter, raw_input, expected, id=f"{name}[{raw_input}]")


def _assert_reproduces(fn, raw_input, expected, *, coerce_non_str):
    value = ast.literal_eval(raw_input)
    if isinstance(expected, dict) and RAISES in expected:
        if coerce_non_str and not isinstance(value, str):
            # Deliberate widening: the old copy raised on a non-string; the new one coerces it.
            assert fn(value) == fn(str(value))
            return
        with pytest.raises(Exception) as excinfo:
            fn(value)
        assert type(excinfo.value).__name__ == expected[RAISES]
        return
    assert fn(value) == expected


def test_fixture_covers_every_former_copy():
    assert set(FIXTURE["doi"]) == set(DOI_MAP)
    assert set(FIXTURE["openalex_id"]) == set(OPENALEX_MAP)
    # The shadowed first db_manager definition was dead code and is deleted.
    assert set(FIXTURE["inverted_index"]) - set(INVERTED_MAP) == {
        "dl_lit.db_manager.DatabaseManager._convert_inverted_index_to_text@L2041(shadowed)"
    }
    assert len(next(iter(FIXTURE["doi"].values()))) == 25
    assert len(next(iter(FIXTURE["openalex_id"].values()))) == 12


@pytest.mark.parametrize("name,shared,getter,raw_input,expected", list(_cases("doi", DOI_MAP)))
def test_doi_copies(db, name, shared, getter, raw_input, expected):
    if shared is not None:
        _assert_reproduces(shared, raw_input, expected, coerce_non_str=True)
    _assert_reproduces(getter(db), raw_input, expected, coerce_non_str=shared is not None)


@pytest.mark.parametrize("name,shared,getter,raw_input,expected", list(_cases("openalex_id", OPENALEX_MAP)))
def test_openalex_id_copies(db, name, shared, getter, raw_input, expected):
    if shared is not None:
        _assert_reproduces(shared, raw_input, expected, coerce_non_str=True)
    _assert_reproduces(getter(db), raw_input, expected, coerce_non_str=shared is not None)


@pytest.mark.parametrize("name,shared,getter,raw_input,expected", list(_cases("inverted_index", INVERTED_MAP)))
def test_inverted_index_copies(db, name, shared, getter, raw_input, expected):
    if shared is not None:
        _assert_reproduces(shared, raw_input, expected, coerce_non_str=False)
    _assert_reproduces(getter(db), raw_input, expected, coerce_non_str=False)


def test_call_sites_alias_the_shared_functions():
    import dl_lit.keyword_search as dl_keyword_search
    import dl_lit.new_dl as new_dl
    import dl_lit.OpenAlexScraper as scraper

    assert scraper.normalize_doi is normalize_doi_lower
    assert scraper.get_abstract is inverted_index_to_text
    assert new_dl.convert_inverted_index_to_text is inverted_index_to_text_lenient
    assert dl_keyword_search.openalex_result_to_record is openalex_work_to_record

    ks = _script("keyword_search")
    assert ks.normalize_doi is extract_doi_lower
    assert ks.normalize_openalex_id is normalize_openalex_id_strict
    assert ks.openalex_result_to_record is openalex_work_to_record
    assert _script("ingest_latest").normalize_doi is normalize_doi
    for name in ("backfill_openalex_topics", "backfill_citation_edges", "migrate_to_works", "graph_3d_export"):
        assert _script(name).normalize_openalex_id is normalize_openalex_id


def test_normalize_doi_case_lower():
    assert normalize_doi("https://doi.org/10.1000/ABC", case="lower") == "10.1000/abc"
    assert normalize_doi("Not-A-Doi", case="lower") == "not-a-doi"
    with pytest.raises(ValueError):
        normalize_doi("10.1000/abc", case="title")


def test_private_doi_copies_differ_from_every_shared_rule():
    shared = (normalize_doi, normalize_doi_lower, extract_doi_lower, lambda v: normalize_doi(v, case="lower"))
    for name in (
        "backend.scripts.backfill_openalex_topics.normalize_doi",
        "backend.scripts.export_bundle.normalize_doi",
        "backend.scripts.upstream_update.normalize_doi",
    ):
        recorded = FIXTURE["doi"][name]
        for fn in shared:
            assert any(
                not (isinstance(out, dict) and RAISES in out) and fn(ast.literal_eval(raw)) != out
                for raw, out in recorded.items()
            ), name


def test_inverted_index_to_text_basic():
    assert inverted_index_to_text({"a": [0], "b": [1]}) == "a b"
    assert inverted_index_to_text({"b": [1], "a": [0]}) == "a b"
    assert inverted_index_to_text({}) is None
    assert inverted_index_to_text(None) is None


def test_openalex_work_to_record_matches_former_mapping():
    item = {
        "id": "https://openalex.org/W1",
        "doi": "https://doi.org/10.1/abc",
        "display_name": "A Title",
        "publication_year": 2020,
        "authorships": [{"author": {"display_name": "Ada"}}, {"author": {}}],
        "abstract_inverted_index": {"x": [0]},
        "keywords": [{"display_name": "k1"}, {"display_name": None}],
        "primary_location": {"source": {"display_name": "J", "publisher": "P"}},
        "biblio": {"volume": "1", "issue": "2", "first_page": "3", "last_page": "9"},
        "type": "article",
        "open_access": {"oa_url": "https://oa"},
    }
    record = openalex_work_to_record(item, run_id=7)
    assert record == {
        "openalex_id": "https://openalex.org/W1",
        "doi": "https://doi.org/10.1/abc",
        "title": "A Title",
        "year": 2020,
        "authors": ["Ada", None],
        "abstract": {"x": [0]},
        "keywords": ["k1"],
        "source": "J",
        "volume": "1",
        "issue": "2",
        "pages": "3--9",
        "publisher": "P",
        "type": "article",
        "url": "https://openalex.org/W1",
        "open_access_url": "https://oa",
        "openalex_json": item,
        "ingest_source": "keyword_search",
        "run_id": 7,
    }
    minimal = openalex_work_to_record({"id": "W2"})
    assert minimal["pages"] is None and minimal["run_id"] is None and minimal["authors"] == []
    assert identifiers.__all__  # module exports are declared
