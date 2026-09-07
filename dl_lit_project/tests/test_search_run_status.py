from dl_lit.db_manager import DatabaseManager


def _db(tmp_path):
    return DatabaseManager(db_path=tmp_path / "t.db")


def test_search_runs_has_status_columns(tmp_path):
    db = _db(tmp_path)
    cols = db._table_columns("search_runs")
    assert {"status", "fetched_count", "expected_count", "error", "finished_at"} <= cols
    db.close_connection()


def test_progress_and_finish_round_trip(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="q", filters={"mode": "query"}, status="running")
    row = db.conn.execute("SELECT status, fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("running", 0, None)

    db.update_search_run_progress(run_id, fetched=400, expected=12000)
    row = db.conn.execute("SELECT fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == (400, 12000)

    db.finish_search_run(run_id, "failed", error="boom")
    row = db.conn.execute("SELECT status, error, finished_at IS NOT NULL FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("failed", "boom", 1)
    db.close_connection()


def test_legacy_run_has_null_status(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="legacy")
    assert db.conn.execute("SELECT status FROM search_runs WHERE id = ?", (run_id,)).fetchone()[0] is None
    db.close_connection()


def test_count_search_results(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="q")
    db.add_search_results(run_id, [{"openalex_id": "W1", "title": "a"}, {"openalex_id": "W2", "title": "b"}])
    assert db.count_search_results(run_id) == 2
    db.close_connection()


def test_add_search_results_dedupes_per_run_and_reports_inserted(tmp_path):
    db = _db(tmp_path)
    run_a = db.create_search_run(query="a")
    run_b = db.create_search_run(query="b")
    assert db.add_search_results(run_a, [{"openalex_id": "W1", "title": "a"}, {"openalex_id": "W2", "title": "b"}]) == 2
    # A later page repeating W2 (and a new W3) inserts only W3.
    assert db.add_search_results(run_a, [{"openalex_id": "W2", "title": "b"}, {"openalex_id": "W3", "title": "c"}]) == 1
    # Duplicates inside one batch collapse too.
    assert db.add_search_results(run_a, [{"openalex_id": "W4", "title": "d"}, {"openalex_id": "W4", "title": "d"}]) == 1
    # Rows without an openalex_id are always kept; the same id in another run is independent.
    assert db.add_search_results(run_a, [{"openalex_id": None, "title": "x"}, {"openalex_id": None, "title": "x"}]) == 2
    assert db.add_search_results(run_b, [{"openalex_id": "W1", "title": "a"}]) == 1
    assert db.count_search_results(run_a) == 6
    assert db.count_search_results(run_b) == 1
    db.close_connection()
