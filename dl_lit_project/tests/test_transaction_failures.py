"""Review fixes 5, task 1: failures roll back, every import outcome prints one summary shape.

PY-36 (create_pending_work IntegrityError), PY-39 (ingest_import_seed summary), PY-52
(add-pending-works exit code), PY-53 (writers roll back non-SQLite errors).
"""
import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner

from dl_lit.cli import cli
from dl_lit.db_manager import DatabaseManager

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"
SUMMARY_KEYS = {"added", "skipped", "corpus_linked", "total", "processed", "stopped", "error", "errors"}


def _load_import_script(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("ingest_import_seed_script", SCRIPTS_DIR / "ingest_import_seed.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _last_json(out):
    return json.loads([line for line in out.strip().splitlines() if line.startswith("{")][-1])


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(str(tmp_path / "t.db"))
    yield manager
    if manager.conn is not None:
        manager.close_connection()


# --- PY-36: create_pending_work undoes its own writes on an IntegrityError --------------------

def _failing_link(db):
    def link(corpus_id, table, work_id, commit=True):
        db.conn.execute("UPDATE works SET title = title WHERE id = ?", (work_id,))  # opens / extends the transaction
        raise sqlite3.IntegrityError("boom")
    return link


def test_integrity_error_commit_true_leaves_no_open_transaction(db, tmp_path, monkeypatch):
    monkeypatch.setattr(db, "add_corpus_item", _failing_link(db))
    row_id, err = db.create_pending_work({"title": "T", "corpus_id": 1})
    assert row_id is None and err.startswith("IntegrityError inserting into works: boom")
    assert db.conn.in_transaction is False

    monkeypatch.undo()
    other_id, other_err = db.create_pending_work({"title": "Other"})
    assert other_err is None and other_id
    second = sqlite3.connect(str(tmp_path / "t.db"))
    try:
        assert second.execute("SELECT COUNT(*) FROM works WHERE title = 'Other'").fetchone()[0] == 1
    finally:
        second.close()


def test_integrity_error_commit_false_undoes_the_entry(db, monkeypatch):
    monkeypatch.setattr(db, "add_corpus_item", _failing_link(db))
    with db.transaction():
        row_id, err = db.create_pending_work({"title": "T2", "corpus_id": 1}, commit=False)
    assert row_id is None and err.startswith("IntegrityError inserting into works: boom")
    assert db.conn.in_transaction is False
    assert db.conn.execute("SELECT COUNT(*) FROM works WHERE title = 'T2'").fetchone()[0] == 0


# --- PY-39: ingest_import_seed always prints one summary shape --------------------------------

def _write_seed(tmp_path, entries):
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps(entries), encoding="utf-8")
    return seed


THREE = [{"title": f"Work {i}", "year": 2000 + i} for i in range(3)]


def test_import_seed_stopped_chunk_prints_summary(tmp_path, monkeypatch, capsys):
    mod = _load_import_script(monkeypatch)
    monkeypatch.setattr(mod, "INGEST_COMMIT_CHUNK", 1)
    real_transaction = DatabaseManager.transaction
    entries = {"n": 0}

    @contextmanager
    def flaky_transaction(self):
        entries["n"] += 1
        if entries["n"] == 2:
            raise sqlite3.OperationalError("database is locked")
        with real_transaction(self):
            yield

    monkeypatch.setattr(DatabaseManager, "transaction", flaky_transaction)
    seed = _write_seed(tmp_path, THREE)
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json", "--corpus-id", "1"])
    out = _last_json(capsys.readouterr().out)
    assert set(out) == SUMMARY_KEYS
    assert out["stopped"] is True and out["processed"] == 1 and out["added"] == 1 and out["total"] == 3
    assert out["corpus_linked"] == 1 and out["skipped"] == 0
    assert out["error"] == "OperationalError: database is locked"


def test_import_seed_parse_failure_uses_the_summary_shape(tmp_path, monkeypatch, capsys):
    mod = _load_import_script(monkeypatch)
    seed = tmp_path / "seed.json"
    seed.write_text("{not json", encoding="utf-8")
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json"])
    out = _last_json(capsys.readouterr().out)
    assert set(out) == SUMMARY_KEYS
    assert out["stopped"] is True and out["processed"] == 0 and out["total"] == 0
    assert (out["added"], out["skipped"], out["corpus_linked"]) == (0, 0, 0)
    assert out["error"].startswith("JSONDecodeError: Failed to parse json:")


def test_import_seed_db_setup_failure_uses_the_summary_shape(tmp_path, monkeypatch, capsys):
    mod = _load_import_script(monkeypatch)
    seed = _write_seed(tmp_path, THREE[:2])
    db_dir = tmp_path / "a_directory"
    db_dir.mkdir()
    mod.main(["--db-path", str(db_dir), "--file", str(seed), "--kind", "json"])
    out = _last_json(capsys.readouterr().out)
    assert set(out) == SUMMARY_KEYS
    assert out["stopped"] is True and out["processed"] == 0 and out["total"] == 2
    assert out["error"].startswith("OperationalError")


def test_import_seed_complete_run_reports_not_stopped(tmp_path, monkeypatch, capsys):
    mod = _load_import_script(monkeypatch)
    seed = _write_seed(tmp_path, THREE)
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json"])
    out = _last_json(capsys.readouterr().out)
    assert set(out) == SUMMARY_KEYS
    assert out["stopped"] is False and out["processed"] == out["total"] == 3 and out["error"] is None
    assert out["added"] == 3


# --- PY-52: add-pending-works exits 1 after an exception --------------------------------------

def test_add_pending_works_exits_1_on_exception(tmp_path, monkeypatch):
    def boom(self, *args, **kwargs):
        raise RuntimeError("x")

    monkeypatch.setattr(DatabaseManager, "add_pending_works_from_json", boom)
    json_file = tmp_path / "refs.json"
    json_file.write_text("[]", encoding="utf-8")
    result = CliRunner().invoke(cli, ["add-pending-works", "--json-file", str(json_file), "--db-path", str(tmp_path / "lit.db")])
    assert result.exit_code == 1 and "x" in result.stderr  # click 8.2 keeps stderr separate
    assert "unexpected error" in result.stderr


# --- PY-53: writers roll back on any exception -------------------------------------------------

class _CursorProxy:
    def __init__(self, cursor, owner):
        self._cursor = cursor
        self._owner = owner

    def execute(self, sql, *args):
        if not self._owner.raised and sql.lstrip().upper().startswith("UPDATE WORKS"):
            self._owner.raised = True
            raise ValueError("bad")
        return self._cursor.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _ConnProxy:
    """Delegates to the real connection; its cursor raises ValueError("bad") on the first UPDATE works."""

    def __init__(self, conn):
        self._conn = conn
        self.raised = False

    def cursor(self, *args):
        return _CursorProxy(self._conn.cursor(*args), self)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _old_work(db):
    work_id = db._insert_work_record({"title": "Old", "metadata_status": "failed", "download_status": "failed"})
    db.conn.commit()
    return work_id


def test_apply_enriched_metadata_rolls_back_non_sqlite_error(db, monkeypatch):
    work_id = _old_work(db)

    def bad_update(*args, **kwargs):
        raise ValueError("bad")

    monkeypatch.setattr(db, "_update_work_record", bad_update)
    with pytest.raises(ValueError):
        db.apply_enriched_metadata(work_id, {"title": "New"})
    assert db.conn.in_transaction is False
    assert db.get_entry_by_id("works", work_id)["title"] == "Old"
    assert db.queue_for_enrichment(work_id) == (work_id, None)


STATUS_COLUMN = {"enqueue_for_download": "download_status", "queue_for_enrichment": "metadata_status"}


@pytest.mark.parametrize("method", ["enqueue_for_download", "queue_for_enrichment"])
def test_queue_writers_roll_back_non_sqlite_error(db, method):
    work_id = _old_work(db)
    column = STATUS_COLUMN[method]
    real = db.conn
    db.conn = _ConnProxy(real)
    try:
        with pytest.raises(ValueError):
            getattr(db, method)(work_id)
    finally:
        db.conn = real
    assert real.in_transaction is False
    assert db.get_entry_by_id("works", work_id)[column] == "failed"
    assert getattr(db, method)(work_id) == (work_id, None)


def test_import_seed_keeps_entry_errors_when_the_commit_fails(tmp_path, monkeypatch, capsys):
    """D25: a failed chunk drops its counts, but `errors` keeps why its entries were skipped."""
    mod = _load_import_script(monkeypatch)
    real_transaction = DatabaseManager.transaction

    @contextmanager
    def commit_fails(self):
        with real_transaction(self):
            yield
            # A deferred foreign-key violation makes the COMMIT itself fail.
            self.conn.execute("PRAGMA defer_foreign_keys = ON")
            self.conn.execute("INSERT INTO search_results (search_run_id, title) VALUES (999, 'orphan')")

    monkeypatch.setattr(DatabaseManager, "transaction", commit_fails)
    seed = _write_seed(tmp_path, ["oops", {"title": "Good", "year": 2001}])
    mod.main(["--db-path", str(tmp_path / "lit.db"), "--file", str(seed), "--kind", "json", "--corpus-id", "1"])
    out = _last_json(capsys.readouterr().out)
    assert out["stopped"] is True and out["error"].startswith("IntegrityError")
    assert (out["added"], out["skipped"], out["corpus_linked"], out["processed"]) == (0, 0, 0, 0)
    assert out["errors"] == ["Skipped non-object entry: str"]
