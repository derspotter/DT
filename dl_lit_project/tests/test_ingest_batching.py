import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

import dl_lit.db_manager as db_manager_module
from dl_lit.db_manager import DatabaseManager

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"

BIB = """
@article{a, title={Alpha Paper}, author={Doe, Jane}, year={2020}, doi={10.1000/alpha}}
@article{b, title={Beta Paper}, author={Roe, Rick}, year={2021}, doi={10.1000/beta}}
@article{c, title={Gamma Paper}, author={Poe, Pat}, year={2022}, doi={10.1000/gamma}}
"""


def _load_import_script(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("ingest_import_seed_script", SCRIPTS_DIR / "ingest_import_seed.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _last_json(out):
    return json.loads([line for line in out.strip().splitlines() if line.startswith("{")][-1])


def test_import_seed_links_existing_works_to_second_corpus(monkeypatch, tmp_path, capsys):
    mod = _load_import_script(monkeypatch)
    db_path = tmp_path / "lit.db"
    bib = tmp_path / "seed.bib"
    bib.write_text(BIB, encoding="utf-8")
    base = ["--db-path", str(db_path), "--file", str(bib), "--kind", "bib"]

    mod.main(base + ["--corpus-id", "1"])
    first = _last_json(capsys.readouterr().out)
    assert first == {"added": 3, "skipped": 0, "corpus_linked": 3, "total": 3, "errors": []}

    mod.main(base + ["--corpus-id", "2"])
    second = _last_json(capsys.readouterr().out)
    assert second == {"added": 0, "skipped": 0, "corpus_linked": 3, "total": 3, "errors": []}

    db = DatabaseManager(db_path)
    try:
        rows = db.conn.execute("SELECT corpus_id, COUNT(*) FROM corpus_works GROUP BY corpus_id").fetchall()
        assert dict(rows) == {1: 3, 2: 3}
        assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 3
    finally:
        db.close_connection()


def _count_commits(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def test_add_pending_works_from_json_batches_commits(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        # Pre-existing works so the batch also hits the duplicate/merge branches.
        for i in range(10):
            db.create_pending_work({"title": f"Existing {i}", "doi": f"10.1000/dup{i}", "year": 2000})
        entries = []
        for i in range(40):
            entries.append({
                "title": f"Fresh Work {i}",
                "authors": [f"Author {i}"],
                "year": 2010,
                "corpus_id": 1,
                "translated_title": f"Translated {i}",
            })
        for i in range(10):
            entries.append({"title": f"Existing {i}", "doi": f"10.1000/dup{i}", "year": 2000, "corpus_id": 1})
        json_path = tmp_path / "refs.json"
        json_path.write_text(json.dumps(entries), encoding="utf-8")

        statements = _count_commits(db)
        added, skipped, errors = db.add_pending_works_from_json(json_path, source_pdf="x.pdf")
        db.conn.set_trace_callback(None)

        assert (added, skipped, errors) == (40, 10, [])
        commits = [s for s in statements if s.strip().upper().startswith(("COMMIT", "END"))]
        assert len(commits) <= 2, commits
        assert not db.conn.in_transaction
        assert db.conn.execute("SELECT COUNT(*) FROM corpus_works WHERE corpus_id = 1").fetchone()[0] == 50
        assert db.conn.execute("SELECT COUNT(*) FROM work_aliases").fetchone()[0] == 40
    finally:
        db.close_connection()


def test_transaction_rolls_back_on_error_and_nests(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        with pytest.raises(RuntimeError):
            with db.transaction():
                with db.transaction():
                    db.add_corpus_item(1, "works", db._insert_work_record({"title": "T"}), commit=False)
                raise RuntimeError("boom")
        assert not db.conn.in_transaction
        assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 0
        assert db.conn.execute("SELECT COUNT(*) FROM corpus_works").fetchone()[0] == 0
    finally:
        db.close_connection()


def _merge_notes(db):
    return [tuple(r) for r in db.conn.execute("SELECT match_field, action, notes FROM merge_log ORDER BY id")]


def _corpus_ids(db, work_id):
    return [row[0] for row in db.conn.execute("SELECT corpus_id FROM corpus_works WHERE work_id = ?", (work_id,))]


def test_create_pending_work_confidence_fields(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        # normalized_doi match: high confidence, reported as duplicate, nothing logged.
        doi_id, _ = db.create_pending_work({"title": "Doi Work", "doi": "10.1000/xyz", "year": 2001})
        row_id, err = db.create_pending_work({"title": "Different", "doi": "https://doi.org/10.1000/XYZ", "corpus_id": 4})
        assert row_id is None and err.endswith("on normalized_doi")
        assert _corpus_ids(db, doi_id) == [4]

        # alias_title match: high confidence as well.
        alias_owner, _ = db.create_pending_work({"title": "Original Title", "year": 1999, "translated_title": "Translated Title"})
        row_id, err = db.create_pending_work({"title": "Translated Title", "year": 1999, "publisher": "Pub"})
        assert row_id is None and err.endswith("on alias_title")
        assert db.get_entry_by_id("works", alias_owner)["publisher"] == "Pub"
        assert _merge_notes(db) == []

        # title_year match: low confidence; still merges, logged as merged, reported as a duplicate.
        ty_id, _ = db.create_pending_work({"title": "Year Only", "year": 2005})
        row_id, err = db.create_pending_work({"title": "Year Only", "year": 2005, "authors": ["Doe, Jane"], "corpus_id": 9})
        assert (row_id, err) == (None, f"Duplicate already exists in works (ID {ty_id}) on title_year")
        assert db.conn.execute("SELECT COUNT(*) FROM works WHERE normalized_title = ?", (db._normalize_text("Year Only"),)).fetchone()[0] == 1
        assert json.loads(db.get_entry_by_id("works", ty_id)["authors"]) == ["Doe, Jane"]
        assert _corpus_ids(db, ty_id) == [9]
        assert _merge_notes(db) == [("title_year", "possible_duplicate", "low_confidence_match_merged")]
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def test_resolution_lookups_run_outside_a_transaction(monkeypatch, tmp_path):
    import dl_lit.OpenAlexScraper as scraper

    db = DatabaseManager(tmp_path / "lit.db")
    seen = []

    def fake_process_single_reference(ref, searcher, rate_limiter, **kwargs):
        seen.append(db.conn.in_transaction)
        return {"doi": "10.1000/resolved", "openalex_id": None}

    monkeypatch.setattr(scraper, "process_single_reference", fake_process_single_reference)
    try:
        db.create_pending_work({"title": "Shared Title", "year": 2000})
        entries = [{"title": "Fresh Before", "year": 1990}, {"title": "Shared Title", "year": 2000}]
        json_path = tmp_path / "refs.json"
        json_path.write_text(json.dumps(entries), encoding="utf-8")

        added, skipped, errors = db.add_pending_works_from_json(
            json_path, searcher=object(), resolve_potential_duplicates=True
        )

        assert (added, skipped, errors) == (1, 1, [])
        assert seen == [False, False]
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def test_transaction_takes_write_lock_before_first_read(tmp_path):
    import sqlite3
    import threading

    db_path = tmp_path / "lit.db"
    db = DatabaseManager(db_path)
    db.conn.execute("PRAGMA busy_timeout = 2000")
    other_done = threading.Event()
    other_errors = []

    def other_writer():
        conn = sqlite3.connect(db_path, timeout=2.0)
        try:
            conn.execute("INSERT INTO works (title) VALUES ('from B')")
            conn.commit()
        except sqlite3.Error as exc:  # pragma: no cover - only on regression
            other_errors.append(exc)
        finally:
            conn.close()
            other_done.set()

    try:
        with db.transaction():
            db.conn.execute("SELECT COUNT(*) FROM works").fetchone()
            thread = threading.Thread(target=other_writer)
            thread.start()
            other_done.wait(0.5)  # with a deferred BEGIN, B commits here and A's write below fails
            db._insert_work_record({"title": "from A"})
        thread.join(5)
        assert other_errors == []
        titles = {row[0] for row in db.conn.execute("SELECT title FROM works")}
        assert titles == {"from A", "from B"}
    finally:
        db.close_connection()


def test_transaction_rolls_back_when_commit_fails(tmp_path):
    import sqlite3

    db = DatabaseManager(tmp_path / "lit.db")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction():
                db.conn.execute("PRAGMA defer_foreign_keys = ON")
                db.conn.execute("INSERT INTO search_results (search_run_id, title) VALUES (999, 'orphan')")
        assert not db.conn.in_transaction
        assert db.conn.execute("SELECT COUNT(*) FROM search_results").fetchone()[0] == 0
    finally:
        db.close_connection()


# --- final wave: chunked commits (I2), per-entry savepoints (I3), merge counters (M1) ----------

def _commits(statements):
    return [s for s in statements if s.strip().upper().startswith(("COMMIT", "END"))]


def test_add_pending_works_from_json_commits_every_chunk(tmp_path):
    from dl_lit.db_manager import INGEST_COMMIT_CHUNK

    assert INGEST_COMMIT_CHUNK == 500
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        entries = [{"title": f"Work {i}", "year": 2000, "corpus_id": 1} for i in range(1200)]
        json_path = tmp_path / "refs.json"
        json_path.write_text(json.dumps(entries), encoding="utf-8")
        statements = _count_commits(db)
        added, skipped, errors = db.add_pending_works_from_json(json_path)
        db.conn.set_trace_callback(None)
        assert (added, skipped, errors) == (1200, 0, [])
        assert len(_commits(statements)) == 3
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def _tracing_import_script(monkeypatch, statements):
    mod = _load_import_script(monkeypatch)
    real = mod.DatabaseManager

    class Tracing(real):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.conn.set_trace_callback(statements.append)

    monkeypatch.setattr(mod, "DatabaseManager", Tracing)
    return mod


def test_import_seed_commits_every_chunk(monkeypatch, tmp_path, capsys):
    statements = []
    mod = _tracing_import_script(monkeypatch, statements)
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps([{"title": f"Work {i}", "year": 2000} for i in range(1200)]), encoding="utf-8")
    del statements[:]
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json", "--corpus-id", "1"])
    summary = _last_json(capsys.readouterr().out)
    assert summary == {"added": 1200, "skipped": 0, "corpus_linked": 1200, "total": 1200, "errors": []}
    assert len(_commits(statements)) == 3


BAD_BATCH = [
    {"title": "Good One", "year": 2001},
    {"title": "Bad Entry", "year": 2002, "translated_title": {"de": "Schlechter Eintrag"}},
    {"title": "Good Two", "year": 2003},
]


def test_add_pending_works_from_json_rolls_back_a_failed_entry(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        json_path = tmp_path / "refs.json"
        json_path.write_text(json.dumps([dict(e, corpus_id=1) for e in BAD_BATCH]), encoding="utf-8")
        added, skipped, errors = db.add_pending_works_from_json(json_path)
        assert (added, skipped, len(errors)) == (2, 1, 1)
        titles = [r[0] for r in db.conn.execute("SELECT title FROM works ORDER BY id")]
        assert titles == ["Good One", "Good Two"]
        assert db.conn.execute("SELECT COUNT(*) FROM corpus_works").fetchone()[0] == 2
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def test_import_seed_rolls_back_a_failed_entry(monkeypatch, tmp_path, capsys):
    mod = _load_import_script(monkeypatch)
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps(BAD_BATCH), encoding="utf-8")
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json", "--corpus-id", "1"])
    summary = _last_json(capsys.readouterr().out)
    assert (summary["added"], summary["skipped"], summary["corpus_linked"]) == (2, 1, 2)
    assert len(summary["errors"]) == 1
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        titles = [r[0] for r in db.conn.execute("SELECT title FROM works ORDER BY id")]
        assert titles == ["Good One", "Good Two"]
        assert db.conn.execute("SELECT COUNT(*) FROM corpus_works").fetchone()[0] == 2
        assert db.conn.execute("SELECT COUNT(*) FROM work_aliases").fetchone()[0] == 0
    finally:
        db.close_connection()


def test_savepoint_rolls_back_only_its_block(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        with db.transaction():
            db._insert_work_record({"title": "kept"})
            with pytest.raises(RuntimeError):
                with db.savepoint():
                    db._insert_work_record({"title": "dropped"})
                    raise RuntimeError("boom")
            with db.savepoint():
                db._insert_work_record({"title": "kept too"})
        assert [r[0] for r in db.conn.execute("SELECT title FROM works ORDER BY id")] == ["kept", "kept too"]
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def test_reimport_with_a_low_confidence_match_links_without_adding(monkeypatch, tmp_path, capsys):
    mod = _load_import_script(monkeypatch)
    first = tmp_path / "first.json"
    first.write_text(json.dumps([{"title": "Year Only", "year": 2005}]), encoding="utf-8")
    second = tmp_path / "second.json"
    second.write_text(json.dumps([{"title": "Year Only", "year": 2005, "authors": ["Doe, Jane"]}]), encoding="utf-8")
    base = ["--db-path", str(tmp_path / "lit.db"), "--kind", "json"]
    mod.main(base + ["--file", str(first), "--corpus-id", "1"])
    capsys.readouterr()
    mod.main(base + ["--file", str(second), "--corpus-id", "2"])
    summary = _last_json(capsys.readouterr().out)
    assert summary == {"added": 0, "skipped": 0, "corpus_linked": 1, "total": 1, "errors": []}
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 1
        assert dict(db.conn.execute("SELECT corpus_id, COUNT(*) FROM corpus_works GROUP BY corpus_id").fetchall()) == {1: 1, 2: 1}
    finally:
        db.close_connection()


# --- second pass: a transaction SQLite rolled back whole fails visibly ----------------------
FILLER_ENTRIES = [{"title": f"Entry {i}", "abstract": "y" * 3000} for i in range(40)]


class _SmallDatabase(DatabaseManager):
    """Writes hit SQLITE_FULL a few pages in, which rolls back the whole transaction."""

    def _create_schema(self):
        super()._create_schema()
        pages = self.conn.execute("PRAGMA page_count").fetchone()[0]
        self.conn.execute(f"PRAGMA max_page_count={pages + 12}")


def _work_count(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM works").fetchone()[0]
    finally:
        conn.close()


def test_add_pending_works_from_json_raises_when_sqlite_drops_the_transaction(tmp_path):
    db_path = tmp_path / "lit.db"
    db = _SmallDatabase(db_path)
    json_path = tmp_path / "refs.json"
    json_path.write_text(json.dumps(FILLER_ENTRIES), encoding="utf-8")
    try:
        with pytest.raises(sqlite3.Error):
            db.add_pending_works_from_json(json_path)
        assert not db.conn.in_transaction
    finally:
        db.close_connection()
    assert _work_count(db_path) == 0  # the chunk is gone as a whole, entries after it never ran


def test_add_pending_works_from_json_counts_never_exceed_the_rows_of_committed_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(db_manager_module, "INGEST_COMMIT_CHUNK", 5)
    db_path = tmp_path / "lit.db"
    db = _SmallDatabase(db_path)
    json_path = tmp_path / "refs.json"
    json_path.write_text(json.dumps(FILLER_ENTRIES), encoding="utf-8")
    try:
        with pytest.raises(sqlite3.Error):
            db.add_pending_works_from_json(json_path)
    finally:
        db.close_connection()
    rows = _work_count(db_path)
    assert rows > 0 and rows % 5 == 0  # earlier chunks stay, the failing one is gone whole


def test_import_seed_fails_when_sqlite_drops_the_transaction(monkeypatch, tmp_path, capsys):
    mod = _load_import_script(monkeypatch)
    monkeypatch.setattr(mod, "DatabaseManager", _SmallDatabase)
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps(FILLER_ENTRIES), encoding="utf-8")
    db_path = tmp_path / "lit.db"
    with pytest.raises(sqlite3.Error):
        mod.main(["--db-path", str(db_path), "--file", str(seed), "--kind", "json", "--corpus-id", "1"])
    assert '"added"' not in capsys.readouterr().out  # no summary claims rows that are gone
    assert _work_count(db_path) == 0


def test_transaction_raises_when_the_transaction_it_opened_is_gone(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        with pytest.raises(sqlite3.OperationalError, match="transaction"):
            with db.transaction():
                db.conn.rollback()
        with db.transaction():  # the connection stays usable
            with db.transaction():  # nested: this call opened nothing, so it checks nothing
                pass
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


# --- second pass: the create path merges title matches whatever their identifiers (spec D5) --
def test_same_title_authors_year_with_a_different_doi_merges_on_create(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    ref = {"title": "Institutions and growth", "authors": ["Jane Roe"], "year": 2020}
    try:
        first_id, err = db.create_pending_work({**ref, "doi": "10.2139/ssrn.111"})
        assert err is None
        row_id, err = db.create_pending_work({**ref, "doi": "10.1000/journal.222", "corpus_id": 7})
        assert row_id is None
        assert err == f"Duplicate already exists in works (ID {first_id}) on title_authors_year"
        assert db.conn.execute("SELECT id, normalized_doi FROM works").fetchall() == [
            (first_id, db._normalize_doi("10.2139/ssrn.111"))
        ]
        assert _corpus_ids(db, first_id) == [7]
    finally:
        db.close_connection()


def test_check_if_exists_guards_identifiers_whenever_a_row_is_excluded(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    ref = {"title": "Same Title", "authors": ["Jane Roe"], "year": 2020}
    try:
        work_id, _ = db.create_pending_work({**ref, "doi": "10.1/one"})
        kwargs = dict(doi="10.1/two", openalex_id=None, title=ref["title"], authors=ref["authors"], year=2020)
        assert db.check_if_exists(**kwargs) == ("works", work_id, "title_authors_year")
        for exclude_table in ("works", None):  # exclude_id alone is enough: the guard cannot be left out
            assert db.check_if_exists(**kwargs, exclude_id=work_id + 1, exclude_table=exclude_table) == (None, None, None)
    finally:
        db.close_connection()


def test_raw_download_does_not_absorb_a_record_with_a_different_doi(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    ref = {"title": "Same Title", "authors": ["Jane Roe"], "year": 2020}
    try:
        first = db._insert_work_record({**db._build_raw_work_payload({**ref, "doi": "10.1/one"})})
        second = db._insert_work_record({**db._build_raw_work_payload({**ref, "doi": "10.1/two"})})
        db.conn.commit()
        result, err = db.mark_raw_work_downloaded(first, {}, "/tmp/one.pdf", "sum", "direct")
        assert (result, err) == (first, None)
        assert db.conn.execute("SELECT COUNT(*) FROM works").fetchone()[0] == 2
        assert db.get_entry_by_id("works", second)["download_status"] != "downloaded"
    finally:
        db.close_connection()


# --- second pass: one non-object entry is skipped, its neighbours survive --------------------
@pytest.mark.parametrize("bad", [5, "oops", None])
def test_add_pending_works_from_json_skips_a_non_object_entry(tmp_path, bad):
    db = DatabaseManager(tmp_path / "lit.db")
    json_path = tmp_path / "refs.json"
    json_path.write_text(json.dumps([{"title": "Good One"}, bad, {"title": "Good Two"}]), encoding="utf-8")
    try:
        added, skipped, errors = db.add_pending_works_from_json(json_path, source_pdf="x.pdf")
        assert (added, skipped) == (2, 1)
        assert len(errors) == 1 and type(bad).__name__ in errors[0]
        assert [r[0] for r in db.conn.execute("SELECT title FROM works ORDER BY id")] == ["Good One", "Good Two"]
    finally:
        db.close_connection()


# --- second pass: a duplicate carrying an identifier owned by another row is still linked ----
def _split_records(db):
    a, _ = db.create_pending_work({"title": "Row A", "authors": ["A"], "year": 2001, "doi": "10.1/a"})
    b, _ = db.create_pending_work({"title": "Row B", "authors": ["B"], "year": 2002, "openalex_id": "W42"})
    return a, b


SPLIT_ENTRY = {"title": "Row A", "authors": ["A"], "year": 2001, "doi": "10.1/a", "openalex_id": "W42"}


def test_import_seed_links_a_duplicate_whose_openalex_id_belongs_to_another_row(monkeypatch, tmp_path, capsys):
    mod = _load_import_script(monkeypatch)
    db_path = tmp_path / "lit.db"
    db = DatabaseManager(db_path)
    a, b = _split_records(db)
    db.close_connection()
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps([SPLIT_ENTRY]), encoding="utf-8")

    mod.main(["--db-path", str(db_path), "--file", str(seed), "--kind", "json", "--corpus-id", "5"])

    summary = _last_json(capsys.readouterr().out)
    assert summary == {"added": 0, "skipped": 0, "corpus_linked": 1, "total": 1, "errors": []}
    db = DatabaseManager(db_path)
    try:
        assert _corpus_ids(db, a) == [5]
        assert db.get_entry_by_id("works", a)["openalex_id"] in (None, "")
        assert db.get_entry_by_id("works", b)["openalex_id"] == "W42"
    finally:
        db.close_connection()


def test_commit_true_merge_leaves_no_open_transaction_on_a_foreign_identifier(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        a, _ = _split_records(db)
        row_id, err = db.create_pending_work({**SPLIT_ENTRY, "corpus_id": 5, "publisher": "Filled In"})
        assert row_id is None and f"(ID {a}) on normalized_doi" in err
        assert _corpus_ids(db, a) == [5]
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


def test_merge_keeps_an_identifier_no_other_row_owns(tmp_path):
    db = DatabaseManager(tmp_path / "lit.db")
    try:
        a, _ = db.create_pending_work({"title": "Row A", "authors": ["A"], "year": 2001, "doi": "10.1/a"})
        db.create_pending_work({"title": "Row A", "authors": ["A"], "year": 2001, "doi": "10.1/a", "openalex_id": "W7"})
        assert db.get_entry_by_id("works", a)["openalex_id"] == "W7"
    finally:
        db.close_connection()
