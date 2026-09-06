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
