import inspect

from dl_lit.db_manager import DatabaseManager


def test_doi_and_openalex_lookups_use_their_partial_indexes(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    cur = db.conn.cursor()
    plans = {}
    for col in ("normalized_doi", "openalex_id"):
        cur.execute(f"EXPLAIN QUERY PLAN SELECT id FROM works WHERE {col} = ? AND {col} != ''", ("x",))
        plans[col] = " ".join(str(r[-1]) for r in cur.fetchall())
    assert "idx_works_normalized_doi" in plans["normalized_doi"]
    assert "idx_works_openalex_id" in plans["openalex_id"]


def test_find_existing_work_query_text_matches_index_predicate():
    src = inspect.getsource(DatabaseManager._find_existing_work)
    assert "normalized_doi = ? AND normalized_doi != ''" in src
    assert "openalex_id = ? AND openalex_id != ''" in src


def test_create_pending_work_resolves_the_existing_work_once(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    calls = []
    orig = db._find_existing_work
    monkeypatch.setattr(db, "_find_existing_work", lambda *a, **k: calls.append(1) or orig(*a, **k))
    new_id, err = db.create_pending_work({"title": "A", "doi": "10.1/a", "year": 2020})
    assert new_id and err is None
    assert len(calls) == 1
