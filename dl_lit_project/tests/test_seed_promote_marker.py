"""The seed marker is written only while the work is in the corpus (PY-55, spec D14)."""

import importlib.util
import sys
from pathlib import Path

import pytest

from dl_lit.db_manager import DatabaseManager

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "backend" / "scripts"

CANDIDATE = {"source_type": "search", "source_key": "run-1", "candidate_key": "cand-1"}


def _script(name):
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(f"{name}_marker_test", SCRIPTS_DIR / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def promote():
    return _script("seed_promote")


@pytest.fixture
def db(tmp_path, promote):
    manager = DatabaseManager(tmp_path / "works.db")
    promote._ensure_seed_membership_schema(manager)
    yield manager
    manager.close_connection()


def _add_member(db, corpus_id=1, work_id=10):
    db.conn.execute("INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)", (corpus_id, work_id))
    db.conn.commit()


def _old_marker(db, work_id=10):
    db.conn.execute(
        """
        INSERT INTO seed_candidates_in_corpus (corpus_id, source_type, source_key, candidate_key, work_id, marked_at)
        VALUES (1, 'search', 'run-1', 'cand-1', ?, '2000-01-01 00:00:00')
        """,
        (work_id,),
    )
    db.conn.commit()


def _markers(db):
    return [
        tuple(row)
        for row in db.conn.execute(
            "SELECT corpus_id, source_type, source_key, candidate_key, work_id, marked_at FROM seed_candidates_in_corpus"
        ).fetchall()
    ]


def test_a_member_work_gets_its_marker(db, promote):
    _add_member(db)
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    markers = _markers(db)
    assert len(markers) == 1
    assert markers[0][:5] == (1, "search", "run-1", "cand-1", 10)


def test_a_removed_work_gets_no_marker(db, promote, capsys):
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    assert _markers(db) == []
    err = capsys.readouterr().err
    assert "[seed_promote]" in err
    assert "cand-1" in err and "10" in err


def test_a_member_marker_writes_no_note(db, promote, capsys):
    _add_member(db)
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    assert capsys.readouterr().err == ""


def test_a_failed_marker_write_is_reported(db, promote, capsys):
    db.conn.execute("DROP TABLE seed_candidates_in_corpus")
    db.conn.commit()
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    assert "[seed_promote] marker write failed:" in capsys.readouterr().err


def test_a_failed_corpus_link_is_reported(db, promote, capsys, monkeypatch):
    def refuse(*_args):
        raise RuntimeError("link refused")

    monkeypatch.setattr(db, "add_corpus_item", refuse)
    promote._add_existing_to_corpus(db, 1, "works", 10)
    err = capsys.readouterr().err
    assert "[seed_promote]" in err and "link refused" in err


def test_a_member_marker_is_refreshed(db, promote):
    _add_member(db)
    _old_marker(db)
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    markers = _markers(db)
    assert len(markers) == 1
    assert markers[0][4] == 10
    assert markers[0][5] != "2000-01-01 00:00:00"


def test_a_non_member_old_marker_is_not_refreshed(db, promote):
    _old_marker(db)
    promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    assert _markers(db) == [(1, "search", "run-1", "cand-1", 10, "2000-01-01 00:00:00")]


def test_the_membership_check_is_part_of_the_insert(db, promote):
    _add_member(db)
    sql = []
    db.conn.set_trace_callback(sql.append)
    try:
        promote._mark_candidate_in_corpus(db, 1, CANDIDATE, 10)
    finally:
        db.conn.set_trace_callback(None)

    insert_at = [i for i, s in enumerate(sql) if s.lstrip().upper().startswith("INSERT")]
    assert len(insert_at) == 1
    insert = sql[insert_at[0]]
    assert "EXISTS (SELECT 1 FROM corpus_works" in insert
    before = sql[: insert_at[0]]
    assert not [s for s in before if s.lstrip().upper().startswith("SELECT") and "corpus_works" in s]
    assert len(_markers(db)) == 1
