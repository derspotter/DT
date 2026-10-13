import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'backend' / 'scripts' / 'corpus_list.py'
spec = importlib.util.spec_from_file_location('corpus_list', SCRIPT)
corpus_list = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus_list)


def test_sort_by_authors_uses_first_author_and_is_case_insensitive():
    items = [
        {'id': 1, 'authors': json.dumps(['zed Last', 'Anne First'])},
        {'id': 2, 'authors': json.dumps(['bob Middle'])},
        {'id': 3, 'authors': 'Alice Plain'},
        {'id': 4, 'authors': None},
    ]
    corpus_list.apply_sort(items, 'authors:asc')
    # Blank sorts first in a plain ascending sort; the UI column is sortable
    # both ways, so we only pin the relative order of the named authors.
    named = [i['id'] for i in items if i['authors']]
    assert named == [3, 2, 1]
    corpus_list.apply_sort(items, 'authors:desc')
    assert [i['id'] for i in items if i['authors']] == [1, 2, 3]


def test_authors_sort_key_is_registered():
    assert 'authors' in corpus_list.SORT_KEYS


def test_query_filter_matches_title_author_publication_only():
    items = [
        {'id': 1, 'title': 'Labour markets', 'authors': 'Ada', 'source': 'J. Econ', 'source_label': 'Search #7'},
        {'id': 2, 'title': 'Other', 'authors': 'Bob', 'source': 'Rev', 'source_label': 'Labour seed'},
    ]
    assert [i['id'] for i in corpus_list.apply_query_filter(items, 'labour')] == [1]
    assert [i['id'] for i in corpus_list.apply_query_filter(items, 'bob')] == [2]
    assert [i['id'] for i in corpus_list.apply_query_filter(items, 'econ')] == [1]
    # The seed/provenance label is shown in its own column but is not a filter field.
    assert corpus_list.apply_query_filter(items, 'search #7') == []


def test_sort_by_date_added_newest_first_blanks_last():
    items = [
        {'id': 1, 'added_at': '2026-09-01 10:00:00'},
        {'id': 2, 'added_at': '2026-10-13 08:30:00'},
        {'id': 3, 'added_at': None},
    ]
    corpus_list.apply_sort(items, 'added:desc')
    assert [i['id'] for i in items] == [2, 1, 3]
    corpus_list.apply_sort(items, 'added:asc')
    assert [i['id'] for i in items][-1] == 2


def test_added_at_never_substitutes_work_creation_inside_a_corpus():
    row = {'corpus_added_at': None, 'created_at': '2026-01-01 00:00:00'}
    assert corpus_list.added_at_for(row, 7) is None
    assert corpus_list.added_at_for({'corpus_added_at': '2026-09-01 10:00:00', 'created_at': '2026-01-01 00:00:00'}, 7) == '2026-09-01 10:00:00'
    # No corpus selected: there is no membership row, so creation is the only date.
    assert corpus_list.added_at_for(row, None) == '2026-01-01 00:00:00'
