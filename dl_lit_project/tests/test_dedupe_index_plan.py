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


def _spy(db, monkeypatch, name):
    calls = []
    orig = getattr(db, name)

    def wrapper(*a, **k):
        result = orig(*a, **k)
        calls.append((a, k, result))
        return result

    monkeypatch.setattr(db, name, wrapper)
    return calls


def test_create_pending_work_doi_match_merges_with_one_lookup(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    first, _ = db.create_pending_work({"title": "A", "doi": "10.1/a", "year": 2020})
    lookups = _spy(db, monkeypatch, "_find_existing_work")

    new_id, err = db.create_pending_work({"title": "A", "doi": "10.1/a", "year": 2020, "publisher": "Pub", "corpus_id": 1})

    # A DOI match is high-confidence: merged into the existing work, no new row.
    assert new_id is None and f"(ID {first}) on normalized_doi" in err
    assert len(lookups) == 1
    assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 1
    assert db.get_entry_by_id("works", first)["publisher"] == "Pub"


def test_create_pending_work_low_confidence_match_resolves_into_the_found_work(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    first, _ = db.create_pending_work({"title": "Same Title", "year": 2020})
    lookups = _spy(db, monkeypatch, "_find_existing_work")
    resolves = _spy(db, monkeypatch, "_resolve_or_create_work")

    new_id, err = db.create_pending_work({"title": "Same Title", "year": 2020, "publisher": "Pub"})

    assert (new_id, err) == (first, None)
    assert len(lookups) == 1
    assert resolves[0][1]["existing"] == (first, "title_year")
    assert resolves[0][2] == (first, "title_year")
    assert db.get_entry_by_id("works", first)["publisher"] == "Pub"


def test_resolve_or_create_work_falls_back_to_a_fresh_lookup_for_a_stale_id(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    gone, _ = db.create_pending_work({"title": "Gone", "year": 2019})
    live, _ = db.create_pending_work({"title": "Live", "doi": "10.1/live", "year": 2020})
    db.conn.execute("DELETE FROM works WHERE id = ?", (gone,))
    db.conn.commit()

    payload = db._build_raw_work_payload({"title": "Live", "doi": "10.1/live", "year": 2020, "publisher": "Pub"})
    assert db._resolve_or_create_work(payload, existing=(gone, "title_year")) == (live, "normalized_doi")
    assert db.get_entry_by_id("works", live)["publisher"] == "Pub"

    # Nothing matches either: a new row is inserted.
    fresh = db._build_raw_work_payload({"title": "Brand New", "year": 2021})
    new_id, field = db._resolve_or_create_work(fresh, existing=(gone, "title_year"))
    assert field is None and new_id not in (gone, live)
    assert db.get_entry_by_id("works", new_id)["title"] == "Brand New"
    assert not db.conn.in_transaction
